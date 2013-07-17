(function(module) {
    /**
     * Shard size incrementor, combines updates if possible.
     *
     * @param zk Zookeeper (zkjs) session instance from coordinator
     * @param {String} key Zookeeper key with shard size information
     * @constructor
     */
    function Incrementor(zk, key) {
        this.zk        = zk;
        this.key       = key;
        this.callbacks = [];
        this.increment = 0;
        this.scheduled = false;
    }

    /**
     * Increment shard size and fire callback on finish.
     *
     * @param {Number} increment Size to increment for
     * @param {Function} callback Callback to fire on finish
     */
    Incrementor.prototype.incrementSize = function(increment, callback) {
        this.schedule(increment, callback);

        if (!this.scheduled) {
            this.update();
        }
    };

    /**
     * Actually perform zookeeper update, internal method.
     */
    Incrementor.prototype.update = function() {
        var self = this,
            increment;

        self.scheduled = true;

        // batch updates for 3 seconds
        setTimeout(function() {
            self.zk.get(self.key, function(error, current, info) {
                self.scheduled = false;

                if (error) {
                    self.fire(error);
                    return;
                }

                current          = JSON.parse(current);
                increment        = self.increment;
                self.increment   = 0;
                current.current += increment;

                self.zk.set(self.key, JSON.stringify(current), info.version, function(error) {
                    if (error == self.zk.errors.BADVERSION) {
                        self.increment += increment;

                        // let's try it again
                        setTimeout(self.update.bind(self), 0);
                        return;
                    }

                    self.fire(error);
                });
            });
        }, 3000);
    };

    /**
     * Fire all scheduled callbacks with specified error, internal method.
     *
     * @param {Error|undefined} error Error to fire
     */
    Incrementor.prototype.fire = function(error) {
        var callbacks = this.callbacks.slice(),
            callback;

        this.callbacks = [];

        while (callback = callbacks.pop()) {
            callback(error);
        }
    };

    /**
     * Schedule shard size update, internal method.
     *
     * @param {Number} increment Size to increment for
     * @param {Function} callback Callback to fire on finish
     */
    Incrementor.prototype.schedule = function(increment, callback) {
        this.increment += increment;
        this.callbacks.push(callback);
    };

    module.exports = Incrementor;
})(module);
