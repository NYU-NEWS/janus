
## Rococo

[![Travis-CI](https://travis-ci.org/msmummy/rococo.svg?branch=master)]
(https://travis-ci.org/msmummy/rococo/)
<!--[![Code Climate](https://codeclimate.com/github/msmummy/rococo/badges/gpa.svg)]
(https://codeclimate.com/github/msmummy/rococo)-->
<!--[![Join the chat at https://gitter.im/msmummy/rococo](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/msmummy/rococo?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)-->

### What is Rococo?

Rococo is a concurrency control protocol for distributed transactions. It aims to improve performance especially for contended workload and keep a strong isolation level, i.e. strict serializability.

### How does Rococo work, and how does it differ? 

We have an OSDI'14 paper: [Extracting More Concurrency from Distributed Transactions](https://www.usenix.org/conference/osdi14/technical-sessions/presentation/mu) that describes the details of Rococo. Here are a few highlights of Rococo's design points.

- Rococo aims to reduce the overhead of blocking and aborting by applying dependency tracking between interfering transactions. Rococo tries not to abort transaction to avoid the extra cost of system aborts and retries.

- The dependency tracking and propagating in Rococo is decentralized, and so is the rest part of Rococo. It does not have a separate layer of server(s) to (pre-)serialize the execution of transactions. The way Rococo does this is to design several states for a transaction, and each state satisfies a few invariants in a collective global state. Thus with a protocol that makes each server carefully shares only a limited part of dependencies with another server, Rococo can make a transaction commit efficiently.

- To support transaction fragments that may generate intermediate results required by later parts of the transaction, Rococo requires an offline checking operated by the workload designers/programmers to determine when a running is safe, and when a running needs to use conventional protocols to merge fragments (and this may lead to sub-aborts inside a transaction). 

### What is in this repository?

This repo mainly contains the codes we used in the paper for evaluation. 

There are a few concurrency control protocols here we used for comparison:

- Two phase locking (2PL), with 2PC for validation and commit. There are three versions of 2PL with different deadlock avoiding mechanisms. 
  * Wound-wait.
  * Wait-die.
  * Timeout.
- Optimistic concurrency control (OCC), with 2PC for validation and commit.
- Rococo. 

There is a TPC-C implementation sharded by districts (rather than by warehouses in common cases) in the repo we used as benchmarks. Note that this TPC-C implementation is tweaked to be more "distributed" so the results should not be compared with stock TPC-C. 

### Setup

There is a simple getting started guide [here](https://github.com/msmummy/rococo/wiki/Getting-Started-Guide).

If you are using Rococo as comparison in your project, we prefer that you email us about your testing setup and results so that we can help make sure that the results are correct.


### License

Rococo uses [SATA License](LICENSE.txt) (Star And Thank Author License, originally [here](https://github.com/zTrix/sata-license)), so you have to star this project before any using:) 
