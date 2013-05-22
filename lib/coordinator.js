(function(module) {
    var util   = require("util"),
        http   = require("http"),
        events = require("events"),
        async  = require("async"),
        nop    = require("nop"),
        Zk     = require("zkjs"),
        redis  = require("redis"),
        Queue  = require("zk-redis-queue"),
        Shard  = require("./shard"),
        Server = require("./server");

    function Coordinator(zk) {
        var self = this;

        self.zk     = zk;
        self.ready  = false;
        self.closed = false;
        self.server = undefined;

        self.shards      = {};
        self.nodes       = {};
        self.servers_map = {};
        self.shards_map  = {};
        self.sizes       = {};
        self.queue       = undefined;

        self.sizes_subscribed = {};

        function zkRestart() {
            if (self.closed) {
                return;
            }

            self.zk.start(function(error) {
                if (error) {
                    self.emit("error", error);
                    setTimeout(zkRestart, 1000);
                } else {
                    self.ready            = true;
                    self.sizes_subscribed = {};

                    (function reloadConfiguration() {
                        var requests = {};

                        requests.servers_map = function reloadServersMap(callback) {
                            if (self.closed) {
                                return;
                            }

                            zk.get("/servers-map", reloadServersMap.bind(this, nop), function(error, map) {
                                if (error) {
                                    self.emit("error", error);
                                    setTimeout(reloadServersMap.bind(this, callback), 1000);
                                    return;
                                }

                                self.servers_map = JSON.parse(map);

                                callback();
                            });
                        };

                        requests.shards_map = function reloadShardsMap(callback) {
                            if (self.closed) {
                                return;
                            }

                            zk.get("/shards-map", reloadShardsMap.bind(this, nop), function(error, map) {
                                if (error) {
                                    self.emit("error", error);
                                    setTimeout(reloadServersMap.bind(this, callback), 1000);
                                    return;
                                }

                                map = JSON.parse(map);

                                Object.keys(map).forEach(function(id) {
                                    self.updateShard(id, map[id]);
                                });

                                async.parallel(Object.keys(map).filter(function(id) {
                                    return !self.sizes_subscribed[id];
                                }).map(function(id) {
                                    self.sizes_subscribed[id] = true;
                                    return self.subscribeForShardSizeUpdates.bind(self, id);
                                }), callback);
                            });
                        };

                        requests.queue = function reloadQueue(callback) {
                            if (self.closed) {
                                return;
                            }

                            zk.get("/queue", reloadQueue.bind(this, nop), function(error, queue) {
                                if (error) {
                                    self.emit("error", error);
                                    setTimeout(reloadQueue.bind(this, callback), 1000);
                                    return;
                                }

                                queue = JSON.parse(queue);

                                function redises(hosts) {
                                    return hosts.map(function(config) {
                                        return redis.createClient(config.port, config.host, {retry_max_delay: 1000});
                                    });
                                }

                                queue = new Queue(redises(queue.servers), new Zk(self.zk.options), queue.key);
                                queue.on("error", self.emit.bind(self, "error"));
                                queue.once("ready", function() {
                                    if (self.queue) {
                                        self.queue.close();
                                    }

                                    self.queue = queue;

                                    callback();
                                });
                            });
                        };

                        async.parallel(requests, function(error) {
                            if (error) {
                                // no errors could happen here, we retry while we can
                                throw error;
                            }

                            self.emit("ready");
                        });
                    })();
                }
            });
        }

        self.zk.on("expired", zkRestart);

        zkRestart();
    }

    util.inherits(Coordinator, events.EventEmitter);

    Coordinator.prototype.subscribeForShardSizeUpdates = function(id, callback) {
        var self    = this,
            retry   = 1,
            watcher;

        if (self.closed) {
            return;
        }

        watcher = self.subscribeForShardSizeUpdates.bind(self, id, nop);

        (function tryAgain() {
            self.zk.get("/shard-size-" + id, watcher, function(error, size) {
                if (error) {
                    // if size node not found, we could wait
                    if (error != self.zk.errors.NONODE) {
                        self.emit("error", error);
                    } else {
                        if (++retry > 3) {
                            self.emit("error", new Error("Shard #" + id + " skipped!"));
                            callback();
                            return;
                        } else {
                            self.emit("error", new Error("Waiting for size of shard #" + id));
                        }
                    }

                    setTimeout(tryAgain, 1000);
                    return;
                }

                retry = 1;

                self.updateShard(id, undefined, JSON.parse(size));

                callback();
            });
        })();
    };

    Coordinator.prototype.updateShard = function(id, info, size) {
        if (size) {
            this.sizes[id] = size;
        }

        if (info) {
            this.shards_map[id] = info;
        }

        console.log("updated shard", id, info, size);

        delete this.shards[id];
    }

    Coordinator.prototype.getNode = function(id) {
        return this.servers_map[id];
    };

    Coordinator.prototype.getShard = function(id) {
        if (!this.shards[id]) {
            this.shards[id] = new Shard(this, id, this.shards_map[id].nodes);
        }

        return this.shards[id];
    };

    Coordinator.prototype.getWriteShard = function(callback) {
        var writable = this.getWritableShards(),
            chances  = [],
            sum      = 0,
            current, chance, i;

        if (Object.keys(writable).length == 0) {
            callback(new Error("No writable shards found!"));
            return;
        }

        Object.keys(writable).forEach(function(id) {
            sum += writable[id];
            chances.push({id: id, free: writable[id]});
        });

        chances.sort(function(left, right) {
            return right.free - left.free;
        });

        chance = Math.random() * sum;

        current = 0;
        for (i in chances) {
            if (!chances.hasOwnProperty(i)) {
                continue;
            }

            current += chances[i].free;
            if (chance <= current) {
                callback(undefined, this.getShard(chances[i].id));
                return;
            }
        }

        callback(new Error("This is not supposed to happen"));
    };

    Coordinator.prototype.getWritableShards = function() {
        var self   = this,
            result = {};

        Object.keys(self.sizes).filter(self.isShardWritable.bind(self)).forEach(function(id) {
            result[id] = 1 - (self.sizes[id].current / self.sizes[id].capacity);
        });

        return result;
    };

    Coordinator.prototype.isShardWritable = function(id) {
        if (this.shards_map[id].read_only) {
            return false;
        }

        return this.sizes[id].current < this.sizes[id].capacity;
    };

    Coordinator.prototype.incrementShardDataSize = function(id, size, callback) {
        var self = this;

        self.zk.get("/shard-size-" + id, function(error, current, info) {
            if (error) {
                callback(error);
                return;
            }

            current = JSON.parse(current);
            current.current += size;

            self.zk.set("/shard-size-" + id, JSON.stringify(current), info.version, function(error) {
                if (error == self.zk.errors.BADVERSION) {
                    // let's try it again
                    setTimeout(self.incrementShardDataSize.bind(self, id, size, callback), 0);
                    return;
                }

                callback(error);
            });
        });
    };

    Coordinator.prototype.enableShard = function(id, callback) {
        var self = this;

        self.zk.get("/shards-map", function(error, map, info) {
            if (error) {
                if (error == zk.errors.NONODE) {
                    callback(new Error("Not initialized!"));
                } else {
                    throw error;
                }
            }

            map = JSON.parse(map.toString());
            if (!map[id]) {
                callback(new Error("Shard #" + id + " not found!"));
                return;
            }

            delete map[id].read_only;

            self.zk.set("/shards-map", JSON.stringify(map), info.version, callback);
        });
    };

    Coordinator.prototype.addShard = function(id, nodes, size, callback) {
        var self = this;

        self.zk.get("/shards-map", function(error, map, info) {
            if (error) {
                if (error == zk.errors.NONODE) {
                    callback(new Error("Not initialized!"));
                } else {
                    callback(error);
                }
            }

            map = JSON.parse(map.toString());

            if (!map[id]) {
                map[id] = {
                    read_only: true // read-only by default
                };
            }

            map[id].nodes = nodes;

            self.zk.set("/shards-map", JSON.stringify(map), info.version, function(error) {
                if (error) {
                    callback(error);
                    return;
                }

                self.zk.create("/shard-size-" + id, JSON.stringify({capacity: size, current: 0}), function(error) {
                    if (error) {
                        if (error != self.zk.errors.NODEEXISTS) {
                            callback(error);
                            return;
                        }
                    }

                    callback();
                });
            });
        });
    };

    Coordinator.prototype.addServer = function(id, host, port, callback) {
        var self = this;

        self.zk.get("/servers-map", function(error, map, info) {
            if (error) {
                if (error == zk.errors.NONODE) {
                    throw new Error("Not initialized!");
                } else {
                    throw error;
                }
            }

            map = JSON.parse(map.toString());

            if (!map[id]) {
                map[id] = {};
            }

            map[id].host = host;
            map[id].port = port;

            self.zk.set("/servers-map", JSON.stringify(map), info.version, callback);
        });
    };

    Coordinator.prototype.setQueueInfo = function(servers, key, callback) {
        var self = this;

        self.zk.get("/queue", function(error, queue, info) {
            if (error) {
                callback(error);
                return;
            }

            queue = JSON.parse(queue);

            queue.servers = servers;
            queue.key     = key;

            self.zk.set("/queue", JSON.stringify(queue), info.version, callback);
        });
    };

    Coordinator.prototype.getStats = function(callback) {
        var self           = this,
            totalSize      = 0,
            usedSize       = 0,
            availableSpace = 0,
            writableShards = 0,
            usageRatio,
            totalShards;

        totalShards = Object.keys(self.shards_map).length;

        Object.keys(self.sizes).forEach(function(id) {
            totalSize += self.sizes[id].capacity;
            usedSize  += self.sizes[id].current;

            if (self.isShardWritable(id)) {
                writableShards += 1;
                availableSpace += self.sizes[id].capacity - self.sizes[id].current;
            }
        });

        if (totalSize) {
            usageRatio = Math.ceil(usedSize / totalSize * 10000) / 10000;
        } else {
            usageRatio = 1;
        }


        self.queue.getSize(function(error, size) {
            if (error) {
                callback(error);
                return;
            }

            callback(undefined, {
                total_size      : totalSize,
                used_size       : usedSize,
                usage_ratio     : usageRatio,
                available_space : availableSpace,
                total_shards    : totalShards,
                writable_shards : writableShards,
                queue_length    : size
            });
        });
    };

    Coordinator.prototype.close = function() {
        this.closed = true;
        this.zk.close();

        if (this.queue) {
            this.queue.close();
        }
    };

    Coordinator.prototype.getServer = function() {
        if (!this.server) {
            this.server = new Server(this);
        }

        return this.server;
    };

    module.exports = Coordinator;
})(module);
