This repository holds implementations of ChainSync: A Synchronization Protocol for Strict Sequential Execution in Linear Distributed Pipelines.
The draft for this RFC can be found here [draft-dohmeyer-chainsync](https://datatracker.ietf.org/doc/draft-dohmeyer-chainsync/)


Abstract:
ChainSync is a lightweight application-layer protocol that runs over
reliable TCP connections to synchronize a fixed linear chain of
distributed processes such that they execute their local tasks in
strict sequential order and only after every process in the chain has
confirmed it is ready.  The protocol has four phases: 1) a forward
"readiness" wave, 2) a backward "start" wave, 3) a forward
"execution" wave, and 4) a backward exit wave.

The design guarantees strict ordering even when nodes become ready at
very different times and requires only point-to-point TCP connections
along the chain, thus no central coordinator is needed.
