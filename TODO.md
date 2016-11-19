

## big tasks (a few weeks)

* investigate the clock sync effects for tapir. 
* look into the seastar framework and replace the rpc layer with it if possible. Because the current rpc layer crash on process failures, we need to either fix it or replace it if we want to test for crash and recovery.
* test failure and recovery.
* implement MDCC.
* implement EPaxos.
* implement Granola.
* implement Calvin.

## mid tasks (one week)

* rewrite the store procedure interface; make it cleaner.

## small tasks (a few days)

* write README and a guide for setup and plot.


## tiny tasks (less than one day)

* rename all brq to janus.
