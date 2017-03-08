
**bold** for ongoing tasks. huge, big, mid, small, tiny for time estimation.

## huge tasks (a few months)

* test failure and recovery. look into the seastar framework and replace the rpc layer with it if possible. Because the current rpc layer crash on process failures, we need to either fix it or replace it if we want to test for crash and recovery.

## big tasks (a few weeks)

* **investigate the clock sync effects for tapir.** 
* implement MDCC.
* implement EPaxos.
* implement Granola.
* implement Calvin.
* implement Sinfornia
* implement CLOCC
* re-implement Rococo.
* try different combinations, e.g. Sinfornia + EPaxos, get them work.
* implement general transaction support for Janus.
* support interactive transaction.

## mid tasks (one week)

* rewrite the store procedure interface; make it cleaner.

## small tasks (a few days)

* **add conflict declaration in tpcc and tpca, shrink the number of pieces in tpcc.**
* **rewrite how janus handles pre-accept request, use the conflict delaration above**
* write README and a guide for setup and plot.

## tiny tasks (less than one day)

* add macros and compile options to disable all the verify
* remove dependency on librt
* ~~write a new marshall deputy~~
* ~~a new external database interface~~
* ~~enable travis-ci (container mode)~~
* ~~rename all brq to janus.~~
