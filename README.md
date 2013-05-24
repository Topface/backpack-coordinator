backpack-coordinator
====

Coordination service for backpack cluster.

### Installation

```
npm backpack-cluster
```

### Usage

This section will show an example where we'll create backpack cluster
from 6 nodes on 2 servers with 2x data replication for failover.

Server will have hostnames `one` and `two`.

Final installation will look like this:

```
  one     two
+-----+ +-----+
+ 001 + + 004 + <- shard #1 on this level
+ 002 + + 005 + <- shard #2 on this level
+ 003 + + 006 + <- shard #3 on this level
+-----+ +-----+
```

#### Installing backpack instances

Please go to [backpack project page](https://github.com/Topface/backpack)
to see how to install backpack instances. Install 6 of them:

* http://one:10001/ (node 001)
* http://one:10002/ (node 002)
* http://one:10003/ (node 003)
* http://two:10004/ (node 004)
* http://two:10005/ (node 005)
* http://two:10005/ (node 006)

#### Setting up coordination service

Let's create coordination service on each physical server:

* http://one:12001/ - coordinator #1
* http://two:12002/ - coordinator #2

Let's assume you have zookeeper service is running on one.local on port 2181.
In real world you'll need 3 or 5 (2n+1 rule) zookeeper instances to eliminate
single point of failure of your cluster.

You also need some redis servers to hold replication queue.
You may have whatever amount you like, but it will affect processing time
(more instances -> more time to process). Remember that if you have one redis then
you make it single point if failure, so make some more.
Let's assume that we have redis instances on one:13001 and two:13002.

Initialize coordinator settings (coordinator package is installed in /opt/bacpack-coordinator):

```
# You may run this without arguments to see help about it

$ /opt/backpack-coordinator/bin/backpack-coordinator-init one.local:2181 /backpack backpack-queue one:13001,two:13002
Servers map initialized
Shards map initialized
Queue initialized
```

Now you may run coordinator service on `one` and `two`:

```
[one] $ /opt/backpack-coordinator/bin/backpack-coordinator one.local:2181 /backpack one 12001
```

```
[two] $ /opt/backpack-coordinator/bin/backpack-coordinator one.local:2181 /backpack two 12002
```

#### Adding capacity

Coordinator nodes will automatically update their configuration (replicators too),
so we may add more backpack nodes on the fly. Let's make 3 shards of 2 nodes each.

We need to register servers first:

```
$ /opt/backpack-coordinator/bin/backpack-coordinator-add-server one.local:2181 /backpack 1 http://one:10001
$ /opt/backpack-coordinator/bin/backpack-coordinator-add-server one.local:2181 /backpack 2 http://one:10002
$ /opt/backpack-coordinator/bin/backpack-coordinator-add-server one.local:2181 /backpack 3 http://one:10003
$ /opt/backpack-coordinator/bin/backpack-coordinator-add-server one.local:2181 /backpack 4 http://two:10004
$ /opt/backpack-coordinator/bin/backpack-coordinator-add-server one.local:2181 /backpack 5 http://two:10005
$ /opt/backpack-coordinator/bin/backpack-coordinator-add-server one.local:2181 /backpack 6 http://two:10006
```

Now register shards. Let's make them 100gb each.

```
$ /opt/backpack-coordinator/bin/backpack-coordinator-add-shard one.local:2181 /backpack 1 1,4 100gb
$ /opt/backpack-coordinator/bin/backpack-coordinator-add-shard one.local:2181 /backpack 2 2,5 100gb
$ /opt/backpack-coordinator/bin/backpack-coordinator-add-shard one.local:2181 /backpack 1 3,6 100gb
```

Shards are added as read-only by default, we need to enable them manually.

```
$ /opt/backpack-coordinator/bin/backpack-coordinator-enable-shard one.local:2181 /backpack 1
```

Good! We're done with setting up coordinators. Set up replicators and you're ready to go!

#### Setting up replication service

Coordinator only upload to one backpack node and create task to replicate data to the rest of them.
You should set up [backpack-replicator](http://github.com/Topface/backpack-replicator) to make it work.

This is very simple, just spawn as many replicators as your load require.
Arguments are zookeeper servers and zookeeper root from backpack-coordinator.
Let's spawn one replicator per physical server.

```
[one] $ /opt/backpack-replicator/bin/backpack-replicator one.local:2181 /backpack
```

```
[two] $ /opt/backpack-replicator/bin/backpack-replicator one.local:2181 /backpack
```

### Uploading files

Just make PUT request to whatever coordinator node you like and receive used shard.

```
$ echo hello, backpack! > hello.txt
$ curl -X PUT -T hello.txt http://two:12002/hi.txt
{"shard_id":"1"}
$ curl http://one:10001/hi.txt
hello, backpack!
```

If get from the first node failed with 404 then you should try next node in shard.
Eventually replicators will put new file to every node in shard.

### Nginx recipe

Real-world application is nginx behind in front of backpack nodes.

Configuration for our case will look like (for host one):

```
upstream backpack-shard-1 {
    server one:10001 max_fails=3 fail_timeout=5s;
    server one:10004 max_fails=3 fail_timeout=5s;
}

upstream backpack-shard-2 {
    server one:10002 max_fails=3 fail_timeout=5s;
    server one:10005 max_fails=3 fail_timeout=5s;
}

upstream backpack-shard-3 {
    server one:10003 max_fails=3 fail_timeout=5s;
    server one:10006 max_fails=3 fail_timeout=5s;
}

server {
    listen one:80;
    server_name one;

    # some reasonable values
    proxy_connect_timeout 5s;
    proxy_send_timeout 5s;
    proxy_read_timeout 10s;

    # retry on the next node if one failed or returned 404
    proxy_next_upstream error timeout http_404 http_502 http_504;

    # this is important as hell
    # don't let anyone upload files from your frontend :)
    if ($request_method !~ ^(GET|HEAD)$ ) {
        return 403;
    }

    # extract shard number and file name from url
    location ~ ^/(.*):(.*)$ {
        set $shard $1;
        set $file  $2;

        proxy_pass http://backpack-shard-$shard/$file;
    }
}
```

With this config you'll be able to downloaded uploaded file by url:
[http://one/1:hi.txt](http://one/1:hi.txt). Start nginx on more than
one physical server to eliminate single point of failure.

### Todo

* [docs] Make docs better.
* [bug] Under heavy load there may be 3 lost items for 30 million processed items.
* [bug] Under heavy load size counter may be lower than actual space usage (can not confirm with 10m items).
* [feature] Having files counter (successful uploads and not) would be nice.

### Authors

* [Ian Babrou](https://github.com/bobrik)
