(function(module) {
    var http = require("http");

    /**
     * Http server for coordinator to handle uploads
     * and serve statistics info.
     *
     * @param {Coordinator} coordinator Coordinator instance to associate with
     * @constructor
     */
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

    /**
     * Handle incoming upload request via PUT method.
     *
     * @param {IncomingMessage} req Incoming PUT request
     * @param {Function} callback Callback with response
     */
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

    /**
     * Handle incoming stats request via GET method.
     *
     * @param {Function} callback Callback with stats
     */
    Server.prototype.statsHandler = function(callback) {
        this.coordinator.getStats(callback);
    };

    /**
     * Return callback that will serialize responses from handlers.
     *
     * @param {ServerResponse} res Response to decorate
     * @returns {Function} Callback to pass in handlers
     */
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

    /**
     * Call listen method on http server, same arguments.
     */
    Server.prototype.listen = function() {
        this.server.listen.apply(this.server, arguments);
    };

    /**
     * Call close method on http server, same arguments.
     */
    Server.prototype.close = function() {
        this.server.close.apply(this.server, arguments);
    };

    module.exports = Server;
})(module);
