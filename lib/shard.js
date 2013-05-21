(function(module) {
    var temp       = require("temp"),
        http       = require("http"),
        fs         = require("fs"),
        async      = require("async"),
        mess       = require("mess"),
        url        = require("url"),
        replicator = require("backpack-replicator");

    function Shard(coordinator, id, nodes) {
        this.coordinator = coordinator;
        this.id          = id;
        this.nodes       = nodes;
        this.replicator  = new replicator(this.coordinator.queue, this.coordinator.servers_map);
    }

    Shard.prototype.acceptWriteRequest = function(req, cb) {
        var self        = this,
            tmp         = temp.createWriteStream(),
            size        = +req.headers['content-length'],
            coordinator = self.coordinator,
            callback;

        callback = function() {
            tmp.close();
            fs.unlink(tmp.path, function(error) {
                if (error) {
                    coordinator.emit("error", error);
                }
            });

            cb.apply(this, arguments);
        };

        req.on("error", callback);
        tmp.on("error", callback);
        req.pipe(tmp);

        // increment shard used space
        coordinator.incrementShardDataSize(self.id, size, function(error) {
            if (error) {
                coordinator.emit("error", error);
            }
        });

        tmp.on("close", self.writeFromFile.bind(self, req.url, size, tmp.path, callback));
    };

    Shard.prototype.writeFromFile = function(path, size, file, callback) {
        var self     = this,
            nodes    = mess(self.nodes.slice()),
            uploaded = [],
            pushers  = [];

        async.doUntil(function(callback) {
            var node = nodes.pop();

            self.writeToNodeFromFile(node, path, size, file, function(error) {
                if (error) {
                    self.coordinator.emit("error", error);
                } else {
                    uploaded.push(node);
                }

                callback();
            });
        }, function() {
            return nodes.length == 0 || uploaded.length > 0;
        }, function() {
            if (nodes.length == 0 && uploaded.length == 0) {
                // we totally fucked up
                callback(new Error("Could not upload file " + path + " to any node: " + self.nodes));
                return;
            }

            self.nodes.forEach(function(id) {
                if (uploaded.indexOf(id) == -1) {
                    pushers.push(function(callback) {
                        console.log("pushing:", {from: uploaded, to: id, path: path});
                        self.replicator.push(uploaded, id, path, callback);
                    });
                }
            });

            async.parallel(pushers, callback);
        });
    };

    Shard.prototype.writeToNodeFromFile = function(id, path, size, file, cb) {
        var node     = this.coordinator.getNode(id),
            stream   = fs.createReadStream(file),
            returned = false,
            request;

        console.log("uploading " + path + " of size " + size + " to server " + id + " from shard " + this.id);

        function callback(error) {
            if (returned) {
                return;
            }

            returned = true;
            cb(error);
        }

        request = http.request({
            host    : node.host,
            port    : node.port,
            path    : path,
            method  : "PUT",
            headers : {
                "Content-Length": size
            }
        });

        request.on("response", function(res) {
            if (res.statusCode != 201 && res.statusCode != 204) {
                res.on("end", function() {
                    callback(new Error("HTTP put failed with code " + res.statusCode + " for " + path + " on " + id));
                });
            } else {
                res.on("end", callback);
            }

            // suck stream in
            res.resume();
        });

        request.on("error", callback);
        stream.on("error", function(error) {
            // because we need to put it somewhere
            stream.unpipe(request)
            stream.resume();

            callback(error);
        });

        stream.pipe(request);
    };

    module.exports = Shard;
})(module);
