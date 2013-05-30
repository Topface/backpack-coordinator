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

    /**
     * Coordinator instance that does shard servers selection and size management.
     *
     * @param zk Zookeeper (zkjs) session instance, not started (will be used exclusively)
     * @constructor
     */
    function Coordinator(zk) {
        var self = this;

        self.zk      = zk;
        self.ready   = false;
        self.closed  = false;
        self.server  = undefined;
        self.created = Date.now();

        self.shards      = {};
        self.nodes       = {};
        self.servers_map = {};
        self.shards_map  = {};
        self.sizes       = {};
        self.queue       = undefined;

        self.stats = {
            processed_uploads : 0,
            processed_bytes   : 0,
            failed_uploads    : 0
        };

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

                        requests.servers_map = self.subscribeForServersMapUpdates.bind(self);
                        requests.shards_map  = self.subscribeForShardsMapUpdates.bind(self);
                        requests.queue       = self.subscribeForQueueInfoUpdates.bind(self);

                        async.parallel(requests, function(error) {
                            if (error) {
                                // no errors could happen here, we retry while we can
                                // let's throw it so everyone will know
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

    /**
     * Subscribe for servers map updates.
     * Must be called only once in constructor.
     *
     * @param {Function} callback Initial callback for "ready" event
     */
    Coordinator.prototype.subscribeForServersMapUpdates = function(callback) {
        var self = this,
            watcher;

        if (self.closed) {
            return;
        }

        watcher = self.subscribeForServersMapUpdates.bind(self, nop);

        self.zk.get("/servers-map", watcher, function(error, map) {
            if (error) {
                self.emit("error", error);
                setTimeout(self.subscribeForServersMapUpdates.bind(self, callback), 1000);
                return;
            }

            self.servers_map = JSON.parse(map);

            callback();
        });
    };

    /**
     * Subscribe for shards map updates.
     * Must be called only once in constructor.
     *
     * @param {Function} callback Initial callback for "ready" event
     */
    Coordinator.prototype.subscribeForShardsMapUpdates = function(callback) {
        var self = this,
            watcher;

        if (self.closed) {
            return;
        }

        watcher = self.subscribeForShardsMapUpdates.bind(self, nop);

        self.zk.get("/shards-map", watcher, function(error, map) {
            if (error) {
                self.emit("error", error);
                setTimeout(self.subscribeForShardsMapUpdates.bind(self, callback), 1000);
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

    /**
     * Subscribe for servers map updates.
     * Must be called only once in constructor.
     *
     * @param {Number} id Shard identifier
     * @param {Function} callback Initial callback for "ready" event
     */
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

    /**
     * Subscribe for queue information map updates.
     * Must be called only once in constructor.
     *
     * @param {Function} callback Initial callback for "ready" event
     */
    Coordinator.prototype.subscribeForQueueInfoUpdates = function(callback) {
        var self = this,
            watcher;

        if (self.closed) {
            return;
        }

        watcher = self.subscribeForQueueInfoUpdates.bind(self, nop);

        self.zk.get("/queue", watcher, function(error, queue) {
            if (error) {
                self.emit("error", error);
                setTimeout(self.subscribeForQueueInfoUpdates.bind(self, callback), 1000);
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

    /**
     * Update shard information about nodes, size or both.
     *
     * @param {Number} id Shard identifier to update
     * @param {Object} info Shard information, must contain "nodes" key
     * @param {Object} size Shard size information, must contain "current" and "capacity" keys
     */
    Coordinator.prototype.updateShard = function(id, info, size) {
        if (size) {
            this.sizes[id] = size;
        }

        if (info) {
            this.shards_map[id] = info;
        }

        delete this.shards[id];
    };

    /**
     * Return node information from actual map, will have
     * "host" and "port" keys in resulting object.
     *
     * @param {Number} id Node identifier
     * @returns {Object}
     */
    Coordinator.prototype.getNode = function(id) {
        return this.servers_map[id];
    };

    /**
     * Return shard object by id. Not recommended to be used more than once.
     *
     * @param {Number} id Shard identifier
     * @returns {Shard}
     */
    Coordinator.prototype.getShard = function(id) {
        if (!this.shards[id]) {
            this.shards[id] = new Shard(this, id, this.shards_map[id].nodes);
        }

        return this.shards[id];
    };

    /**
     * Callback will receive {Shard} object that may be used for writing.
     *
     * @param {Function} callback Callback to receive shard object
     */
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

    /**
     * Return object where keys are shard identifiers
     * and values are free space ratio.
     *
     * @returns {Object}
     */
    Coordinator.prototype.getWritableShards = function() {
        var self   = this,
            result = {};

        Object.keys(self.sizes).filter(self.isShardWritable.bind(self)).forEach(function(id) {
            result[id] = 1 - (self.sizes[id].current / self.sizes[id].capacity);
        });

        return result;
    };

    /**
     * Check is specified shard writable.
     *
     * @param {Number} id Shard identifier
     * @returns {boolean}
     */
    Coordinator.prototype.isShardWritable = function(id) {
        if (this.shards_map[id].read_only) {
            return false;
        }

        return this.sizes[id].current < this.sizes[id].capacity;
    };

    /**
     * Increment shard size. Will retry automatically
     * to keep size consistent.
     *
     * @param {Number} id Shard identifier
     * @param {Number} size Size in bytes to add
     * @param {Function} callback Callback to call after increment
     */
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

    /**
     * Remove "read-only" label from shard to enable write requests.
     *
     * @param {Number} id Shard identifier
     * @param {Function} callback Callback to call after update
     */
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

    /**
     * Add shard with id, nodes and capacity information.
     * Added shard will be read-only. If shard already exists then
     * only nodes for it will be updated, size will stay the same.
     *
     * @param {Number} id Shard identifier
     * @param {Array} nodes Array of node identifiers for shard
     * @param {Number} capacity Shard capacity in bytes
     * @param {Function} callback Callback for errors
     */
    Coordinator.prototype.addShard = function(id, nodes, capacity, callback) {
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

                self.zk.create("/shard-size-" + id, JSON.stringify({capacity: capacity, current: 0}), function(error) {
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

    /**
     * Add server (node) with specified host and port.
     * If node already exists, it will be updated with new values.
     *
     * @param {Number} id Server identifier
     * @param {String} host Hostname of the node
     * @param {Number} port Port of the node
     * @param {Function} callback Callback for errors
     */
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

    /**
     * Update queue information such as servers and queue key.
     *
     * @param {Array} servers Array of objects with "host" and "port" keys
     * @param {String} key Queue key to hold information
     * @param {Function} callback Callback for errors
     */
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

    /**
     * Return stats information in callback:
     * - uptime Coordinator uptime in milliseconds
     * - processed_uploads Files uploaded since start
     * - processed_bytes Bytes uploaded since start
     * - failed_uploads Failed uploads since start
     * - total_size Capacity in bytes (including read-only shards)
     * - used_size Used space in bytes
     * - usage_ratio Ratio for used space (0..1)
     * - available_space Available space in bytes (excluding read-only shards)
     * - total_shards Total amount of shards
     * - writable_shards Amount of writable shards
     * - queue_length Current queue length for replication
     *
     * @param {Function} callback Callback to receive statistics
     */
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
                uptime            : Date.now() - self.created,
                processed_uploads : self.stats.processed_uploads,
                processed_bytes   : self.stats.processed_bytes,
                failed_uploads    : self.stats.failed_uploads,
                total_size        : totalSize,
                used_size         : usedSize,
                usage_ratio       : usageRatio,
                available_space   : availableSpace,
                total_shards      : totalShards,
                writable_shards   : writableShards,
                queue_length      : size
            });
        });
    };

    /**
     * Close coordinator connections for zookeeper and queue.
     * No more requests should be sent to this coordinator after this.
     */
    Coordinator.prototype.close = function() {
        this.closed = true;
        this.zk.close();

        if (this.queue) {
            this.queue.close();
        }
    };

    /**
     * Return wrapped http server to handle uploads.
     *
     * @returns {Server}
     */
    Coordinator.prototype.getServer = function() {
        if (!this.server) {
            this.server = new Server(this);
        }

        return this.server;
    };

    module.exports = Coordinator;
})(module);
