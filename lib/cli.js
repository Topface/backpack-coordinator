(function(module) {
    var ZK          = require("zkjs"),
        Coordinator = require("./coordinator");

    module.exports.start = function() {
        var args = process.argv.slice(2),
            coordinator;

        if (args.length < 4) {
            console.error("Usage: backpack-coordinator <zk_servers> </zk/root> <listen_host> <listen_port>")
            process.exit(1);
        }

        coordinator = this.makeCoordinator(args[0], args[1]);
        coordinator.on("error", console.error.bind(console));
        coordinator.on("ready", function() {
            console.log("coordinator is ready!");

            coordinator.getServer().listen(args[3], args[2]);
            console.log("Listening to http://" + args[2] + ":" + args[3] + "/");
        });
    };

    module.exports.makeCoordinator = function(servers, root) {
        return new Coordinator(new ZK({hosts: servers.split(","), root: root}));
    };

})(module);
