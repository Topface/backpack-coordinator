(function(module) {
    var ZK            = require("zkjs"),
        Coordinator   = require("./coordinator"),
        os            = require("os"),
        statsdCreator = require('uber-statsd-client');

    module.exports.start = function() {
        const StatsGaugeInterval = 10000;
        var args = process.argv.slice(2),
            hostname = os.hostname(),
            coordinator,
            statsd;

        if (args.length < 4) {
            console.error("Usage: backpack-coordinator <zk_servers> </zk/root> <listen_host> <listen_port> [<statsd_endpoint>]")
            process.exit(1);
        }

        if (args[4]) {
            var statsdEndpoint = args[4].split(":")
            statsd = statsdCreator({
                host: statsdEndpoint[0],
                port: statsdEndpoint[1] || 8125,
                prefix: 'backpack-coordinator.' + hostname
            });
        } else {
            statsd = statsdCreator({
                isDisabled: function() {
                    return true;
                }
            });
        }

        var writeStats = function(statsd, coordinator) {
            // stats by shards
            Object.keys(coordinator.sizes).forEach(function(id) {
                var key = id + (coordinator.isShardWritable(id) ? ".rw" : ".ro");
                statsd.gauge(key + ".current_size", coordinator.sizes[id].current);
                statsd.gauge(key + ".capacity_size", coordinator.sizes[id].capacity);
                statsd.gauge(key + ".available_size", coordinator.sizes[id].capacity - coordinator.sizes[id].current);
            });

            // stats by whole cluster
            coordinator.getStats(function(error, data) {
                if (error) {
                    console.log(error);
                    return;
                }

                statsd.gauge("total_size", data.total_size);
                statsd.gauge("used_size", data.used_size);
                statsd.gauge("queue_length", data.queue_length);
                statsd.gauge("total_shards", data.total_shards);
                statsd.gauge("available_space", data.available_space);
            })
        };

        coordinator = this.makeCoordinator(args[0], args[1]);
        coordinator.on("error", function(error) {
            if (typeof error != "object") {
                error = new Error(error);
            }

            console.log((new Date().toString()) + " " + error.stack);
        });
        coordinator.on("ready", function() {
            console.log("coordinator is ready!");

            coordinator.getServer().listen(args[3], args[2]);
            console.log("Listening to http://" + args[2] + ":" + args[3] + "/");

            // send internal stats each 10 seconds
            setInterval(function() {
                writeStats(statsd, coordinator);
            }, StatsGaugeInterval)
        });

        // send stats about an every added photo
        coordinator.on("photo.add", function(error, time) {
            var key = error ? "add_photo.error" : "add_photo.ok";
            statsd.increment(key);
            statsd.timing(key, time)
        });
    };

    module.exports.makeCoordinator = function(servers, root) {
        return new Coordinator(new ZK({hosts: servers.split(","), root: root}));
    };

})(module);
