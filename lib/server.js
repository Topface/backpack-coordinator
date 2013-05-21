(function(module) {
    var http = require("http");

    function Server(coordinator) {
        var self = this;

        self.coordinator = coordinator;

        self.server = http.createServer(function(req, res) {
            var callback = self.responseHandler(res);

            if (req.method == "PUT") {
                self.uploadHandler(req, callback);
            } else if (req.method == "GET") {
                if (req.url == "/stats") {
                    self.statsHandler(callback);
                } else {
                    callback(new Error("Unknown url for GET: " + req.url));
                }
            } else {
                callback(new Error("Unsupported method: " + req.method));
            }
        });
    }

    Server.prototype.uploadHandler = function(req, callback) {
        this.coordinator.getWriteShard(function(error, shard) {
            if (error) {
                callback(error);
                return;
            }

            shard.acceptWriteRequest(req, function(error) {
                if (error) {
                    callback(error);
                    return;
                }

                callback(undefined, {shard_id: shard.id});
            });
        });
    };

    Server.prototype.statsHandler = function(callback) {
        this.coordinator.getStats(callback);
    };

    Server.prototype.responseHandler = function(res) {
        return function(error, result, code) {
            var headers = {"Content-type": "application/json"};

            if (!code) {
                code = 200;
            }

            if (error) {
                res.writeHead(500, headers);
                result = {error: error + ""};
            } else {
                res.writeHead(code, headers);
            }

            res.end(JSON.stringify(result) + "\n");
        };
    };

    Server.prototype.listen = function() {
        this.server.listen.apply(this.server, arguments);
    };

    Server.prototype.close = function() {
        this.server.close.apply(this.server, arguments);
    };

    module.exports = Server;
})(module);
