------------------------------- MODULE Janus -------------------------------

EXTENDS Naturals, Sequences, TLC, FiniteSets
CONSTANT M, N, X \* N partitions, in X-way replica

\*CONSTANT OP
\*CONSTANT PIE
CONSTANT TXN
\*CONSTANT TXN_POOL
  
CONSTANT Shard(_)

GetTxShards(tx) == 
{s \in 1..N: \exists i \in DOMAIN tx: s=tx[i].shard}

DEFAULT_KEY_SPACE == 
  [i \in {"a", "b", "c"} |-> 
     [last_tx_meta |-> 
        [id |-> 0, shards |-> {}], 
      alogs |-> {}] ]

(* Do you know anybody who knows anybody who knows anybody
   who knows anybody who knows how TLC works? *)
BoundedSeq(S, n) == UNION {[1..i -> S] : i \in 0..n}
BSeq(S) == BoundedSeq(S, Cardinality(S)+1)

MAX(m, n) == IF m > n THEN m ELSE n

(* status space of a transaction *)
UNKNOWN      == 0
PRE_ACCEPTED == 1
PREPARED     == 2 \* prepared and accepted  
ACCEPTED     == 3 \* compare ballot 
COMMITTING   == 4
DECIDED      == 5

FAST_QUORUM == X
SLOW_QUORUM == X \div 2 + 1 

DEFAULT_ACCEPT_BALLOT == 1

(* serialization graph *)

IsSrzGraph(G) == (* True iff G is a srz graph *)
  /\ G = [node |-> G.node, edge |-> G.edge]
  /\ G.edge \subseteq (G.node \X G.node)

SubSrzGraph(G) == 
  { H \in [node : SUBSET G.node, edge : SUBSET (G.node \X G.node)] :
      /\ IsSrzGraph(H) 
      /\ H.edge \subseteq G.edge
      /\ \forall m \in H.node, n \in H.node: 
        /\ <<m, n>> \in G.edge => <<m, n>> \in H.edge 
        /\ <<m, n>> \in G.edge => <<m, n>> \in H.edge
  }
  
EmptySrzGraph ==
  [node |-> {}, edge |-> {}]  
  
SrzAddNode(G, node) == 
  [G EXCEPT !.node = @ \union {node}]

SrzAddEdge(G, m, n) == 
  [G EXCEPT !.edge = @ \union {<<m, n>>}]

SrzPath(G) == 
  { p \in BoundedSeq(G.node, Cardinality(G.node)+1) : 
      /\ p /= <<>>
      /\ \forall i \in 1 .. (Len(p)-1) : 
           <<p[i], p[i+1]>> \in G.edge}
  
SrzCyclic(G) == 
  \exists p \in SrzPath(G) : /\ Len(p) > 1
                             /\ p[1] = p[Len(p)]
                            
SrzAcyclic(G) == ~SrzCyclic(G)


(* data types and operators for dependency graph *)       

TypeTxMeta(i) == (* True iff id is a transaction meta type *)
  /\ i = [n |-> i.n, shard |-> i.shard]
  /\ i.n \in Nat
  /\ i.shards \in Nat
  
GenTxMeta(i, s) == 
  [id |-> i, shards |-> s]

XsXXX(g) == 
  (DOMAIN g) \X (DOMAIN g)

EmptyNewDep == 
  <<>>

IdsFromMetas(metas) == 
  {m.id : m \in metas}

NewDepEdgeSet(g) == 
  {edge \in XsXXX(g): edge[1] \in IdsFromMetas(g[edge[2]].parents)}

NewDepPathSet(g) == 
  {p \in BSeq(DOMAIN g) : /\ p /= <<>>
                          /\ \forall i \in 1 .. (Len(p)-1) : 
                               <<p[i], p[i+1]>> \in NewDepEdgeSet(g)}

NewDepCyclic(g) == 
  \exists p \in NewDepPathSet(g) : /\ Len(p) > 1
                                   /\ p[1] = p[Len(p)]                        

NewDepAcyclic(g) == ~NewDepCyclic(g)

IsNewDepVertex(V) == 
  /\ V = [
            tid |-> Nat, 
            status |-> {UNKNOWN, PRE_ACCEPTED, PREPARED, 
                        ACCEPTED, COMMITTING, DECIDED}, 
            max_prepared_ballot |-> Nat, 
            max_accepted_ballot |-> Nat, 
            parents |-> SUBSET Nat,
            partitions |-> SUBSET Nat
         ]
  
IsNewDepGraph(G) == 
  /\ \forall v \in G: IsNewDepVertex(v)
  

\*  SUBSET G
SubNewDep(dep, tid_set) == 
  [t \in tid_set |-> dep[t]] 

NewDepExistsVertex(G, vid) == 
\*  \exists v \in G: v.tid = vid
  vid \in DOMAIN G

NewDepAddEdge(g, from, to) == 
  IF NewDepExistsVertex(g, to) THEN 
    [g EXCEPT ![to].from = @ \union {from}]       
  ELSE
    FALSE \* assert 0;
\*  
\*(* dependency graph *) 
\*IsDepGraph(G) == (* True iff G is a dep graph. *)
\*  /\ G = [node |-> G.node, 
\*          edge |-> G.edge, 
\*          status |-> G.status, 
\*          partitions |-> G.partitions, 
\*          max_ballot |-> G.max_ballot]
\*  /\ G.edge \subseteq (G.node \X G.node)
\*  /\ G.status \in [i \in G.node |-> {UNKNOWN, PRE_ACCEPTED, PREPARED, ACCEPTED, COMMITTING, DECIDED}]
\*  /\ G.partitions \in [i \in G.node |-> SUBSET {M+1 .. M+N}]
\*  /\ G.max_ballot \in [i \in G.node |-> Nat]
\*
\*SubDepGraph(G) == (* The set of all subgraphs of a dep graph. 
\*                     Note: All edges that belong to the included nodes are included. *)
\*  { H \in [node : SUBSET G.node, edge : SUBSET (G.node \X G.node \X {"i", "d"})] :
\*      /\ IsDepGraph(H) 
\*      /\ H.edge \subseteq G.edge
\*      /\ \forall m \in H.node, n \in H.node: /\ <<m, n, "i">> \in G.edge => <<m, n, "i">> \in H.edge 
\*                                             /\ <<m, n, "d">> \in G.edge => <<m, n, "d">> \in H.edge
\*  }
\*
\*DepAddNode(G, node, status, partitions) == 
\*  IF node \in G.node 
\*    THEN [G EXCEPT !.status[node] = MAX(@, status), !.partitions[node] = @ \union partitions]
\*    ELSE [G EXCEPT !.node = @ \union {node}, !.status[node] = status, !.partitions[node] = partitions]
\*        
\*DepAddEdge(G, edge) == 
\*  [G EXCEPT !.edge = @\union edge]

MAX_BALLOT_STATUS(b1, s1, b2, s2) ==
  IF (s1 = PREPARED \/ s1 = ACCEPTED) /\ (s2 = PREPARED \/ s2 = ACCEPTED) THEN
    IF b1 > b2 THEN
      s1
    ELSE
      s2
  ELSE
    MAX(s1, s2)
      
NewDepSize(G) == 
  Cardinality(DOMAIN G)

\*DepAggregate(G1, G2) == 
\*  [
\*    node |-> G1.node \union G2.node, 
\*    edge |-> G1.edge \union G2.edge, 
\*    status |-> [m \in G1.node \union G2.node |-> 
\*                 IF m \in G1.node THEN 
\*                   IF m \in G2.node THEN 
\*                     MAX_BALLOT_STATUS(G1.ballot[m], 
\*                                       G1.status[m], 
\*                                       G2.ballot[m], 
\*                                       G2.status[m]) 
\*                   ELSE 
\*                     G1.status[m] 
\*                 ELSE
\*                     G2.status[m]
\*               ],
\*    ballot |-> [m \in G1.node \union G2.node |-> 
\*                 IF m \in G1.node THEN 
\*                   IF m \in G2.node THEN 
\*                     MAX(G1.ballot[m], G2.ballot[m]) 
\*                   ELSE 
\*                     G1.ballot[m] 
\*                 ELSE
\*                     G2.ballot[m]
\*               ],           
\*    partitions |-> [m \in G1.node \union G2.node |-> 
\*                 IF m \in G1.node THEN
\*                   IF m \in G2.node THEN
\*                     G1.partitions[m] \union G2.partitions[m]
\*                   ELSE
\*                     G1.partitions[m]
\*                 ELSE
\*                   G2.partitions[m]
\*               ]
\*  ]
\*  


AreDirectlyConnectedIn(m, n, G) == 
  \exists p \in NewDepPathSet(G) : (p[1] = m) /\ (p[Len(p)] = n) /\ Len(p) = 1

AreConnectedIn(m, n, G) == 
  \exists p \in NewDepPathSet(G) : (p[1] = m) /\ (p[Len(p)] = n)
    
AreStronglyConnectedIn(m, n, G) ==
    /\ AreConnectedIn(m, n, G)
    /\ AreConnectedIn(n, m, G)
    
\*IsStronglyConnected(G) == 
\*  \forall m, n \in DOMAIN G : m /= n => AreConnectedIn(m, n, G)

NewDepIsStronglyConnected(dep, tid_set) == 
  \forall m, n \in tid_set: m/=n => AreConnectedIn(m, n, dep)

(* Compute the Strongly Connected Component of a node *)
SccTidSet(dep, tid) == 
  CHOOSE tid_set \in SUBSET (DOMAIN dep):
    /\ tid \in tid_set
    /\ NewDepIsStronglyConnected(dep, tid_set)
    /\ \forall m \in DOMAIN dep:
         AreStronglyConnectedIn(m, tid, dep) => m \in tid_set
\*    
\*StronglyConnectedComponent(G, node) == 
\*  CHOOSE scc \in SubNewDepGraph(G) : 
\*    /\ node \in DOMAIN scc
\*    /\ IsStronglyConnected(scc)
\*    /\ \forall m \in DOMAIN G : 
\*        AreStronglyConnectedIn(m, node, G) => m \in DOMAIN scc

FindOrCreateTx(dep, tid, meta) == 
  IF tid \in DOMAIN dep THEN
    dep[tid] 
  ELSE
    [tid |-> tid,
     meta |-> meta,
     status |-> UNKNOWN, 
     max_prepared_ballot |-> 0,
     max_accepted_ballot |-> 0,
     parents |-> {},
     partitions |-> {}]

TempOp(acks) == 
  [x \in acks |-> x.dep]

CommitReady(accept_acks, partitions) == 
  IF \forall p \in partitions: 
    Cardinality(accept_acks[p]) >= SLOW_QUORUM THEN TRUE ELSE FALSE  

UpdateBallot(dep, tid, ballot, status) == 
  [dep EXCEPT ![tid].status = status, ![tid].max_prepared_ballot = ballot]

FastPathPossible(pre_accept_acks, partitions) == 
  IF \exists p \in partitions:
    /\ p \in DOMAIN pre_accept_acks 
    /\ \exists acks \in SUBSET pre_accept_acks[p]:
       /\ Cardinality(acks) > X - FAST_QUORUM + 1
       /\ \forall m, n \in acks: m.dep /= n.dep THEN
    FALSE
  ELSE 
    TRUE
  
FastPathReady(arg_pre_accept_acks, arg_shards) == 
  IF \forall p \in arg_shards:
    /\ p \in DOMAIN arg_pre_accept_acks 
    /\ \exists acks \in SUBSET arg_pre_accept_acks[p]:
       /\ Cardinality(acks) >= FAST_QUORUM
       /\ \forall m, n \in acks: m.dep = n.dep THEN
    TRUE
  ELSE 
    FALSE
  
PickXXX(acks) == 
  CHOOSE a \in acks: 
    \exists q \in SUBSET acks: 
      /\ Cardinality(q) >= FAST_QUORUM
      /\ \forall m, n \in q: m.dep = n.dep    

SlowPathReady(pre_accept_acks, partitions) == 
  \forall p \in partitions: 
    /\ p \in DOMAIN pre_accept_acks
    /\ Cardinality(pre_accept_acks[p]) >= SLOW_QUORUM

----------------------------------------------
(* messaging functions *)

PartitionIdToProcIds(p) == 
  M+(p-1)*X+1 .. M+p*X

InitAcks(partitions) == 
  [i \in partitions |-> {}]

\* proc M + (par_id-1) *X, M + par_id * X
BroadcastMsg(mq, p, msg) == 
  [i \in M+1 .. M+N*X |-> IF i \in PartitionIdToProcIds(p) THEN 
                            Append(mq[i], msg) 
                          ELSE mq[i]]

ProcIdToPartitionId(proc_id) == 
  (proc_id - M - 1) \div X + 1 

PartitionsToProcIds(partitions) ==
  UNION {ids \in SUBSET (M+1..M+N*X): 
            \exists p \in partitions: 
              ids = PartitionIdToProcIds(p)}  
\*  CHOOSE ids \in SUBSET{M+1 .. M+N*X}:
\*    \forall p \in M+1..M+N*X:
\*      \/ /\ p \in partitions
\*         /\ \forall x \in M+(p-1)*X..M+p*X: x \in ids
\*      \/ /\ p \notin partitions
\*         /\ \forall x \in M+(p-1)*X..M+p*X: x \notin ids        

BroadcastMsgToMultiplePartitions(mq, partitions, msg) == 
  [i \in M+1 .. M+N*X |-> IF i \in PartitionsToProcIds(partitions) THEN 
                            Append(mq[i], msg) 
                          ELSE 
                            mq[i]]                               

(* Retrieve the status of a transaction by its id from a dep graph *)
GetStatus(dep, tid) == 
  dep[tid].status

(* Update the status of a transaction to a newer status by its id *)    
UpdateStatus(dep, tid, status) == 
  IF dep[tid].status < status THEN [dep EXCEPT ![tid].status = status] ELSE dep
    
UpdateSubGraphStatus(g1, tid_set, status) == 
  IF \exists i \in tid_set: i \notin (DOMAIN g1) THEN
    FALSE \* do not support for now
  ELSE
    [
      tid \in DOMAIN g1 |->
        IF tid \in tid_set THEN
          [g1[tid] EXCEPT !.status = status]
        ELSE 
          g1[tid]
    ] 

IsInvolved(dep, tid, par) == (* Judge if a transaction involves this server *)
    IF par \in dep[tid].partitions THEN TRUE ELSE FALSE


\*UndecidedAncestor(G, tid) == 
\*  CHOOSE anc \in SubNewDepGraph(G) : 
\*    \forall m \in G.node : 
\*      ( /\ AreConnectedIn(m, tid, G) 
\*        /\ GetStatus(G, tid) < DECIDED ) 
\*        => m \in anc.node

NewDepUpdateVertex(dep, curr_txn) == 
  [i \in (DOMAIN dep) \union {curr_txn.tid} |->
    IF i \in {curr_txn.tid} THEN 
      curr_txn
    ELSE
      dep[i]
  ]

\*NearestDependencies(dep, tid) == 
\*  CHOOSE anc \in SubNewDepGraph(dep) :
\*    /\ \forall m \in dep.node:
\*      AreDirectlyConnectedIn(m, tid, dep) => m \in anc.node
\*    /\ \forall m \in anc.node: AreDirectlyConnectedIn(m, tid, dep)  

\*DowngradeExcept(dep, tid) == 
\*  [dep EXCEPT !.status = [m \in dep.node |-> (IF m /= tid THEN UNKNOWN ELSE dep.status[m])]]
\*    

\*NearestDependenciesWithDowngrade(dep, tid) == 
\*  DowngradeExcept(NearestDependencies(dep, tid), tid)

AreAllAncestorsAtLeastCommitting(G, tid) ==
  \forall m \in DOMAIN G : AreConnectedIn(m, tid, G) 
                             => GetStatus(G, m) >= COMMITTING

AllAncestorsLowerThanCommittingTidSet(G, tid) == 
  CHOOSE tid_set \in SUBSET (DOMAIN G):
    \forall m \in DOMAIN G: 
      ( /\ AreConnectedIn(m, tid, G) 
        /\ GetStatus(G, tid) < COMMITTING ) 
        <=> m \in tid_set
    
\*AllAncestorsLowerThanCommitting(G, tid) ==
\*  CHOOSE anc \in SubNewDepGraph(G) : 
\*    \forall m \in DOMAIN G: 
\*      ( /\ AreConnectedIn(m, tid, G) 
\*        /\ GetStatus(G, tid) < COMMITTING ) 
\*        => m \in DOMAIN anc


AllAncestorsAtLeastCommittingTidSet(G) == 
  CHOOSE H \in SUBSET DOMAIN G : 
    /\ \forall m \in DOMAIN G : 
         ( /\ G[m].status = COMMITTING
           /\ AreAllAncestorsAtLeastCommitting(G, m) )
                <=> m \in H 

    
Finished(finished_map, tid) == 
  IF tid \in DOMAIN finished_map THEN
    finished_map[tid]
  ELSE
    FALSE                                           

Unfinished(finished_map, tid) == 
  ~Finished(finished_map, tid)

AreSubGraphAncestorsAtLeastCommitting(G, tid_set) == 
  \forall m \in DOMAIN G : 
    ( /\ m \notin tid_set 
      /\ \exists n \in tid_set : AreConnectedIn(m, n, G)
    ) 
    => G[m].status >= COMMITTING
                            
AreSubGraphLocalAncestorsFinished(G, tid_set, par, finished_map) ==
  \forall m \in DOMAIN G : 
    ( /\ m \notin tid_set 
      /\ \exists n \in tid_set : AreConnectedIn(m, n, G)
      /\ par \in G[m].partitions
    ) 
    => finished_map[m] = TRUE
                                                                                                                                            
    
\* DecidedUnfinishedLocalSCCWithAllAncestorsCommittingAndAllLocalAncestorsFinished
\*ToExecuteSCC(G, sid, finished_map) == 
\*  CHOOSE scc \in SubNewDepGraph(G) : 
\*    /\ IsStronglyConnected(scc)
\*    /\ \exists tid \in DOMAIN scc: scc = StronglyConnectedComponent(G, tid)
\*    /\ \forall tid \in DOMAIN scc: scc[tid].status = DECIDED
\*    /\ \exists tid \in DOMAIN scc: /\ IsInvolved(scc, tid, sid) 
\*                                   /\ Unfinished(finished_map, tid) 
\*    /\ AreSubGraphAncestorsAtLeastCommitting(G, scc)
\*    /\ AreSubGraphLocalAncestorsFinished(G, scc, sid, finished_map)

ToExecuteSccTidSet(G, par, finished_map) == 
  { 
    scc \in SUBSET DOMAIN G : 
      /\ NewDepIsStronglyConnected(G, scc)
      /\ \exists tid \in scc: scc = SccTidSet(G, tid)
      /\ \forall tid \in scc: G[tid].status = DECIDED
      /\ \exists tid \in scc: /\ IsInvolved(G, tid, par) 
                              /\ Unfinished(finished_map, tid) 
      /\ AreSubGraphAncestorsAtLeastCommitting(G, scc)
      /\ AreSubGraphLocalAncestorsFinished(G, scc, par, finished_map) 
  }                            

\*ToExecuteSCCSet(G, sid, finished_map) ==
\*  { 
\*    scc \in SubNewDepGraph(G) : 
\*      /\ IsStronglyConnected(scc)
\*      /\ \exists tid \in DOMAIN scc: scc = StronglyConnectedComponent(G, tid)
\*      /\ \forall tid \in DOMAIN scc: scc[tid].status = DECIDED
\*      /\ \exists tid \in DOMAIN scc: /\ IsInvolved(scc, tid, sid) 
\*                                     /\ Unfinished(finished_map, tid) 
\*      /\ AreSubGraphAncestorsAtLeastCommitting(G, scc)
\*      /\ AreSubGraphLocalAncestorsFinished(G, scc, sid, finished_map) 
\*  }                            

MinVid(vid_set) == 
  CHOOSE x \in vid_set: \A t \in vid_set: x >= t
  

\*NextToExe(scc) == 
\*  CHOOSE t \in scc.node : \forall tid \in DOMAIN scc: tid >= t
  
-----------------------------------------------------
IsMsgValid(msg) == 
  /\ msg.src \in Nat
  /\ msg.type \in {"pre_accept", 
                   "accept", 
                   "commit", 
                   "inquire",
                   "ack_pre_accept",
                   "end"}

CheckTxnIdsGtZero(ids) == 
  \forall i \in ids: i > 0


-----------------------------------------------------
(* Transaction model related*)

IsOpSet(opset) ==
  \A pair \in opset:
    /\ pair.key \in {"a", "b", "c"}
    /\ pair.type \in {"r", "w"} 

IsPieRequest(pie) == 
  \forall i \in DOMAIN pie: /\ IsOpSet(pie.opset)
  

IsTxnRequest(txn) == 
  \forall i \in DOMAIN txn: /\ i \in Nat
                            /\ IsPieRequest(txn[i])

-----------------------------------------------------
(* Offline checking *)

Conflict(p, q) == 
  \exists op1 \in p, op2 \in q: /\ op1.key = op2.key
                                /\ ( \/ op1.type /= op2.type
                                     \/ op1.type = "w" )

ConnectedS(TxnSet, p, q) ==
    \exists T \in TxnSet: /\ p \in T
                          /\ q \in T

ConnectedC(TxnSet, p, q) ==
    \exists T, K \in TxnSet: /\ T /= K
                             /\ p \in T
                             /\ q \in K
                             /\ Conflict(p, q)

ConnectedCI(TxnSet, p, q) ==
    /\ ConnectedC(TxnSet, p, q)
    /\ p.iord = "i"                         

SCIPath(TxnSet) ==
  { path \in Seq(UNION(TxnSet)): 
      /\ \forall i \in 1..Len(path): \/ ConnectedS(TxnSet, path[i], path[i+1])
                                     \/ ConnectedCI(TxnSet, path[i], path[i+1])
      /\ \exists i \in 1..Len(path): ConnectedS(TxnSet, path[i], path[i+1])
      /\ \exists i \in 1..Len(path): ConnectedCI(TxnSet, path[i], path[i+1])
  }  

ImmediacyProp(TxnSet) == 
  \forall T, K \in TxnSet: 
    \forall p \in T, q \in K: Conflict(p,q) => p.iord = q.iord

HasImmediateSCCycle(TxnSet) == 
  \exists path \in SCIPath(TxnSet): 
    /\ path[1] = path[Len(path)]
    /\ \forall i, j \in 1..Len(path)-1: i/=j => path[i] /= path[j]

OfflineCheck(TxnSet) == /\ ImmediacyProp(TxnSet)
                        /\ ~HasImmediateSCCycle(TxnSet)

-------------------------------------------------------------------------
(*
--fair algorithm Janus {
  variables coo_mq = [i \in 1 .. M |-> <<>>]; \* Assume there are M coordinators. 
            \* par_mq = [i \in 1 ..
            svr_mq = [i \in M .. M+N*X |-> <<>>]; \* Assume each partition has X servers, N partitions in total.
            srz = EmptySrzGraph;
            n_committed = 0;
            id_counter = 0;
                      
  procedure UnitTest() {
    __debug_label_kldjf: assert(SrzAcyclic(EmptySrzGraph));
    test_label_1: tmp_partitions := PartitionsToProcIds({1});
    if (M = 1 /\ X = 1) {
      debug_label_dslfkj: assert(tmp_partitions = {2});
    } else if (M = 1 /\ X = 2) {
      debug_label_lklfkj: assert(tmp_partitions = {2,3});    
    };
\*    debug_label_lkjl: assert(Cardinality(Seq({1, 2})) = 4);
\*    debug_label_ooi: tmp_partitions := Seq({1});
\*    debug_label_dflkjlkj: assert(tmp_partitions = {<<1>>});
    lbl_dbg_dksfjlas: return;
  }
            
  procedure AggregateGraphForFastPath(pre_accept_acks, partitions) {
    xxxxdddx: 
    tmp_partitions := partitions;   
    yyyyassy: while (tmp_partitions /= {}) {
      with (p \in tmp_partitions) {
        tmp_par := p;
        tmp_partitions := @ \ {p};
      };
      tmp_ack := PickXXX(pre_accept_acks[tmp_par]);
      dep_c := @ \union tmp_ack.dep;
    };
    return;
  }       
  
  procedure AggregateGraphForAccept(pre_accept_acks, partitions) {
    slowpath_init: 
    tmp_partitions := partitions;
    slowpath_scan_acks: while (tmp_partitions /= {}) {
      with (p \in tmp_partitions) {
        tmp_acks := pre_accept_acks[p];
        tmp_partitions := @ \ {p};
        
      };
      zzzzz: while (tmp_acks /= {}) {
        with (a \in tmp_acks) {
          dep_c := @ \union a.dep;
          tmp_acks := @ \ {a};         
        }
      }
    };
    return;
  }
  
  procedure HandlePreAccept() {
    lbl_on_pre_accept: 
    curr_txn.status := PRE_ACCEPTED;
    curr_pie := msg_in.pie;
    pie_map[msg_in.tid] := @ \union {msg_in.pie};
    opset := curr_pie.opset;
    assert(opset /= {});
    lbl_pie_add_dep: 
    while (opset /= {}) {
      with (next_op \in opset) {
        opset := opset \ {next_op};
        curr_op := next_op;      
      };
      (* ignore read without hurting correctness *)
      tx_meta := keyspace[curr_op.key].last_tx_meta;
      if (tx_meta.id /= 0) {
        curr_txn.parents := @ \union {tx_meta};
      };
      keyspace[curr_op.key].last_tx_meta := curr_txn.meta;
    };
    (* reply dependencies *)
    curr_txn.partitions := {ProcIdToPartitionId(self)};
    dep := NewDepUpdateVertex(dep, curr_txn);            
\*    reply_dep: ya_dep := SingleVertexNewDep(dep, curr_txn.tid);
    msg_out := [type |-> "ack_pre_accept", 
                tid |-> curr_txn.tid, 
                src |-> self, 
                dep |-> curr_txn.parents];
    \*assert(CheckTxnIdsGtZero(curr_txn.parents));
    coo_mq[msg_in.src] := Append(coo_mq[msg_in.src], msg_out);
    return;
  }
  
  procedure HandlePrepare() {
    lbl_on_prepare:
    dep := UpdateBallot(dep, curr_txn.tid, msg_in.ballot, PREPARED);
    msg_out := [type |-> "ack_prepare", src |-> self, tid |-> msg_in.tid, ballot |-> msg_in.ballot];
    lbl_on_prep_reply: svr_mq[msg_in.src] := Append(coo_mq[msg_in.src], msg_out);
    return;
  }
  
  procedure HandleAccept() {
    lbl_on_accept_1:
    dep[msg_in.tid].status := ACCEPTED;    
    lbl_on_accept_2:
    dep[msg_in.tid].parents := msg_in.dep;
\*    dep := UpdateBallot(dep, curr_txn.tid, msg.ballot, ACCEPTED);
    msg_out := [type |-> "ack_accept", src |-> self, tid |-> msg_in.tid, ballot |-> msg_in.ballot];
    coo_mq[msg_in.src] := Append(coo_mq[msg_in.src], msg_out);
    return;
  }
  
  procedure InquireUnknown() {
    lbl_inquire_unknown:
    tx_meta_set := msg_in.dep;
    lbl_at_inquire: 
    while (tx_meta_set /= {}) {
      with (meta \in tx_meta_set) {
        assert(meta.id > 0);
        tx_meta_set := @ \ {meta};
        if (ProcIdToPartitionId(self) \in meta.shards) {
          skip;
        } else {
          assert(FALSE);
          with (dst \in meta.shards) {
            msg_out := [type |-> "inquire", tid |-> meta.id, src |-> self];
            svr_mq := BroadcastMsg(svr_mq, dst, msg_out);
            assert(FALSE); \* disalbe for now.
            skip;
          } 
        }                      
      }                
    };
    return;
  }
  
  procedure ExeSccSet() {
    lbl_exe_scc_set:
    vid_set := scc;
    assert(vid_set /= {});        
    lbl_exe_scc: 
    while (vid_set /= {}) {
      next_tid := MinVid(scc);
      vid_set := vid_set \ {next_tid};
      if ( /\ IsInvolved(dep, next_tid, ProcIdToPartitionId(self)) 
           /\ Unfinished(finished_map, next_tid) ) {
        (* execute and add into serialization graph *)
        __debug_label_zcxvx: assert(pie_map[next_tid] /= {});
        if (M = 1) {
          __debug_label_kdjf: assert(next_tid = 1);
        };
        exe_all_deferred_pies: while (pie_map[next_tid] /= {}) {
          with (pie \in pie_map[next_tid]) {
            next_pie := pie;                                    
            pie_map[next_tid] := @ \ {next_pie};
          };
          call ExePie(next_pie);                         
        };
    
        finished_map[next_tid] := TRUE;
    \*              __debug_label_sdkddfj: assert(Finished(finished_map, 1));            
        msg_out := [type |-> "ack_commit", 
                    tid |-> next_tid, 
                    src |-> self];
        \* TODO use coo_id as txn_id for now;           
        coo_mq[next_tid] := Append(@, msg_out);
        skip;   
      } else {
        if (M=1) {
          \* cannot happen with single client.        
          assert(FALSE);
        }
      }
    };
    return;
  }
  
  procedure HandleCommit() {
    lbl_on_commit:
\*    lbl_cmt_set_statsu: 
\*    dep[msg_in.tid].status := COMMITTING; \* TODO upgrade instead of set.
\*    lbl_cmt_set_dep: 
\*    dep[msg_in.tid].parents := msg_in.dep;
    dep[msg_in.tid] := [@ EXCEPT !.status = COMMITTING, !.parents = msg_in.dep];
    (* send inquire request for uninvovled & uncommitted ancestors *)              
    call InquireUnknown(); \* disable for now.      
    (* Trigger decision for those transactions whose status are COMMITTING 
       and their ancestors all become at least committing *)            
    lbl_choose_to_commit: 
    txn_set := AllAncestorsAtLeastCommittingTidSet(dep);
\*    if (M=1) {
\*      assert(txn_set /= {});
\*    };
    lbl_ready_to_commit: 
    while (txn_set /= {}) {
      with (txn \in txn_set) {
        tid_set := SccTidSet(dep, txn);
        txn_set := txn_set \ {txn};
        (* every txn in the scc should become DECIDED *)
        dep := UpdateSubGraphStatus(dep, tid_set, DECIDED);
      }            
    };            
    lbl_find_exe: 
    scc_set := ToExecuteSccTidSet(dep, ProcIdToPartitionId(self), finished_map);
\*      if ( M = 1 /\ N = 1 /\ X = 1) {
\*        __debug_label_kdfjldf: tmp_set := SUBSET DOMAIN dep;      
\*        __debug_label_lejrjkk: assert(tmp_set = {{}, {1}});
\*        __debug_label_dskfjld: assert(NewDepIsStronglyConnected(dep, {1})); 
\*        __debug_label_dsfldjk: assert(SccTidSet(dep, 1) = {1});
\*        __debug_label_dlfjdlf: assert(IsInvolved(dep, 1, 1));
\*      };
\*      __debug_label_dlfjlfk: assert(Unfinished(finished_map, 1));
\*      __debug_label_dlfjldf: assert(AreSubGraphAncestorsAtLeastCommitting(dep, {1}));
\*      __debug_label_ljkdjfj: assert(AreSubGraphLocalAncestorsFinished(dep, {1}, 1, finished_map));
\*      /\ \forall tid \in scc: G[tid].status = DECIDED
\*      /\ \exists tid \in scc: /\ IsInvolved(G, tid, par) 
\*                              /\ Unfinished(finished_map, tid) 
\*      /\ AreSubGraphAncestorsAtLeastCommitting(G, scc)
\*      /\ AreSubGraphLocalAncestorsFinished(G, scc, par, finished_map) 
      
\*      __debug_label_kzcjvli: assert(scc_set /= {});  
\*      lbl_exe_scc_set: 
    if (scc_set /= {}) {
      lbl_next_exe: 
      while (scc_set /= {}) {                
        with (next_scc \in scc_set) {
          scc_set := scc_set \ {next_scc};
          scc := next_scc;
        };                          
        call ExeSccSet();
      };
\*      __debug_label_sdkfj: assert(Finished(finished_map, 1));
      goto lbl_find_exe;
    } else {
      skip;               
    };                    
    lbl_on_commit_ret: 
    return;
  }
  
  procedure HandleInquire() {
    lbl_on_inquire:
    return;
  }         
  
  procedure ExePie(curr_pie)
  {
    lbl_exe_pie: 
    opset := curr_pie.opset;
    assert(opset /= {});
\*    assert(FALSE);              
    lbl_pie_add_srz: 
    while (opset /= {}) {
      with (next_op \in opset) {
        curr_op := next_op;
        opset := opset \ {next_op};                   
      };  
      srz := SrzAddNode(srz, curr_txn.tid);
      alogs := keyspace[curr_op.key].alogs;                       
      lbl_scan_alogs: 
      while (alogs /= {}) {
        with (next_access \in alogs) {
          alogs := alogs \ {next_access};
          if ( \/ next_access.type /= curr_op.type 
               \/ next_access.type = "w") {
            srz := SrzAddEdge(srz, next_access.tid, curr_txn.tid);   
          };
          keyspace[curr_op.key].logs := @ \union {<<curr_txn.tid, curr_op.type>>};
        }
      };  
    };
    return;
  }  
    
  process(Coo \in 1 .. M)
  variables next_txn; 
            next_pie;
            txn_id;
            dep_c;
            par;
            n_pie; 
            pre_accept_acks;
            accept_acks;
            committed;
            ballot;
            tmp_ack;
            tmp_acks;
            tmp_par;
            tmp_partitions;
            shards;
            msg_in;
            msg_out;
  {
\*    unit_test: call UnitTest();       
    lbl_coord_begin: 
    (* cleanup work *)
    dep_c := {};
    shards := {};  
    id_counter := id_counter + 1;
    \* use coord. id for txn_id for now
    txn_id := self;
    assert(txn_id > 0);    
    committed := FALSE; 
    (* do the next transaction*)
    next_txn := TXN[self];
    assert(IsTxnRequest(next_txn));
    n_pie := Len(next_txn);    
    assert(n_pie > 0);
    lbl_get_shards:
    shards := GetTxShards(next_txn);
    assert(shards /= {});
    (* coord pre-accept phase *) 
    lbl_pre_accept:
    while (Len(next_txn) > 0) {
      next_pie := Head(next_txn);
      next_txn := Tail(next_txn);
      par := Shard(next_pie);
      msg_out := [
                    type |-> "pre_accept", 
                    src |-> self, 
                    tid |-> txn_id, 
                    pie |-> next_pie,
                    meta |-> GenTxMeta(txn_id, shards)
                 ];
      assert(IsMsgValid(msg_out));                 
      svr_mq := BroadcastMsg(svr_mq, par, msg_out);
    };
    \* wait for acks. 
    pre_accept_acks := InitAcks(shards);
\*    assert(pre_accept_acks = [i \in shards |-> {}]);
    lbl_coord_pre_accept_ack: 
    while (TRUE) { \* this is a dead loop
      await Len(coo_mq[self]) > 0;
      msg_in := Head(coo_mq[self]); 
      coo_mq[self] := Tail(coo_mq[self]);
      assert(IsMsgValid(msg_in));
      if (msg_in.type = "ack_pre_accept" /\ msg_in.tid = txn_id) {
        par := ProcIdToPartitionId(msg_in.src);
\*        lbl_dbg_aaaaa: assert(par \in shards);
        pre_accept_acks[par] := pre_accept_acks[par] \union {msg_in};
\*        lbl_deg_bbbbb: assert(pre_accept_acks[par] /= {});
\*        assert(pre_accept_acks[par] /= <<>>);
        if (FastPathPossible(pre_accept_acks, shards)) {
          if (FastPathReady(pre_accept_acks, shards)) {
            call AggregateGraphForFastPath(pre_accept_acks, shards);
            lbl_coord_goto_commit: 
            goto commit_phase;
          } else {
            lbl_dbg_zzzzz: 
            if (N=1 /\ X=1) {
              assert(FALSE);
            }
          };
        } else {
          if (M = 1) {
            assert(FALSE);          
          }
        };
\*        lbl_dbg_yyyyy: assert(FALSE);
\*        disable accept phase for now. TODO perhaps bugs here.
\*        lbl_coord_slow_path_check: if (SlowPathReady(pre_accept_acks, shards)) {
\*          call AggregateGraphForAccept(pre_accept_acks, shards);
\*          goto_accept: goto accept_phase;
\*        };
        \* if nothing is possible, waiting for more messages to arrive.
      } else {
        \* impossible
        assert(FALSE);
      };
    };
    
    \* TODO
    (* coord accept phase*)
    accept_phase: ballot := DEFAULT_ACCEPT_BALLOT; 
    msg_out := [
      type |-> "accept",
      ballot |-> DEFAULT_ACCEPT_BALLOT,
      src |-> self, 
      tid |-> txn_id,
      dep |-> dep_c
    ];
    lbl_dbg_jjjjj: assert(msg_out.ballot \geq 0);
    broadcast_accept: svr_mq := BroadcastMsgToMultiplePartitions(svr_mq, shards, msg_out);
    accept_acks := InitAcks(shards);
    process_accept_ack: while(TRUE) { \* this is a dead loop
      await Len(coo_mq[self]) > 0;
      msg_in := Head(coo_mq[self]); 
      coo_mq[self] := Tail(coo_mq[self]);      
      if (/\ msg_in.type = "ack_accept" 
          /\ msg_in.tid = txn_id 
          /\ msg_in.ballot = ballot) {
        par := ProcIdToPartitionId(msg_in.src);
        accept_acks[par] := @ \union {msg_in};
        if (CommitReady(accept_acks, shards)) {
          goto commit_phase;
        }; \* else just wait for message
        \* TODO if nothing is possible then find something to do.
      } else {
        \* could be an outdated message
        if (msg_in.type /= "ack_pre_accept") {
          __debug_lable_gkdjfk: assert(FALSE); \* TODO delete this.
        }
      }
    };
    
    commit_phase: \* graph[txn_id].partitions := partitions;
    \* commit_phase_set_status: \* graph[txn_id].status := COMMITTING;
    msg_out := [
      type |-> "commit",
      src |-> self, 
      tid |-> txn_id,
      dep |-> dep_c
    ]; 
\*    __debug_label_ghjkk: assert(msg_out.dep[txn_id].partitions = {1});
    assert(IsMsgValid(msg_out));
\*    __debug_label_ljhklhjk: assert(partitions = {1});
\*    broadcast_commit: 
    svr_mq := BroadcastMsgToMultiplePartitions(svr_mq, shards, msg_out);            
    lbl_wait_commit_ack: 
    while (TRUE) { 
      await (Len(coo_mq[self]) > 0);
      msg_in := Head(coo_mq[self]);
      coo_mq[self] := Tail(coo_mq[self]);                 
      if (msg_in.type = "ack_commit" /\ msg_in.tid = txn_id) {
        (* COMMITED *)
        committed := TRUE;
        n_committed := n_committed + 1;
        \* assert(n_committed = M);
        goto end_coord;
      } else {
        lbl_dbg_kkkkk: 
        assert(msg_in.type /= "ack_commit");
      }
    };       
        
   \* end server loop to avoid eventual deadlock.
    end_coord: 
    if (n_committed = M) {
      msg_out := [type|->"end", src|->self, tid |-> 0];
      \* assert(PartitionsToProcIds({1}) = {2});
      svr_mq := BroadcastMsgToMultiplePartitions(svr_mq, 1..N, msg_out);
    }
  }
    
  process (Svr \in M+1 .. M+N*X) 
  variables keyspace = DEFAULT_KEY_SPACE;
            dep = EmptyNewDep;  
            ya_dep; 
            finished_map = [i \in 1..M |-> FALSE]; 
            asked_map; 
            pie_map = [i \in 1..M |-> {}]; 
            tx_meta,
            scc, 
            scc_set; 
            scc_seq; 
            curr_txn;
            curr_pie;
            curr_op;            
            txn_set;
            tid_set;
            opset; 
            next_txn;
            next_tid;
            next_pie;
            anc; 
            tmp_set;
            alogs;
            vid_set;
            sub_graph;
            tx_meta_set;
            msg_in;
            msg_out;
            svr_end=FALSE;
  {
    lbl_svr_loop: 
    await Len(svr_mq[self]) > 0;
    msg_in := Head(svr_mq[self]);
    svr_mq[self] := Tail(svr_mq[self]);
    \* lbl_dbg_msg_check: assert(IsMsgValid(msg_in));
    ya_dep := EmptyNewDep;
    \* curr_txn := [tid |-> msg.tid, status |-> "unknown", partitions |-> msg.partitions];        
    \* dep := DepAddNode(dep, curr_txn.tid, UNKNOWN, msg.partitions);
    if (msg_in.tid > 0) {    
      curr_txn := FindOrCreateTx(dep, msg_in.tid, msg_in.meta);
    } else {
      assert(msg_in.type = "end");
    };
\*    if (msg_in.type = "accept") {
\*      __debug_label_kjkjdf: assert(msg_in.ballot \geq 0);
\*      __debug_label_fdsadf: assert(curr_txn.max_prepared_ballot \geq 0);
\*    };
\*    lbl_dbg_ddddd: assert(msg_in.tid /= 2);   \* TODO errors might happen here (message layer)              
\*    lbl_msg_dispatch: 
    if ( /\ msg_in.type = "pre_accept" 
         /\ curr_txn.max_prepared_ballot = 0 
         /\ curr_txn.status = UNKNOWN) {
      call HandlePreAccept();       
    } else if ( \/ msg_in.type = "commit" 
                \/ msg_in.type = "ack_inquire") {
      call HandleCommit();
    } else if (msg_in.type = "inquire") {
      skip; \* enable inquire later;
\*      await GetStatus(dep, msg_in.tid) >= COMMITTING;
\*\*      inquire_ack: ya_dep := SingleVertexNewDep(dep, msg.tid);                
\*      msg_out := [type |-> "ack_inquire", src |-> self, tid |-> msg_in.tid, dep |-> ya_dep];
\*      svr_mq[msg_in.src] := Append(coo_mq[msg_in.src], msg_out);
    } else if (msg_in.type = "prepare" /\ curr_txn.max_prepared_ballot < msg_in.ballot) {
      call HandlePrepare();   
    } else if (msg_in.type = "accept" /\ curr_txn.max_prepared_ballot \leq msg_in.ballot) {
      call HandleAccept();
    } else if (msg_in.type = "end") {
      svr_end := TRUE;
    } else {
      assert(FALSE);
    };
    lbl_svr_loop_end: 
    if (~svr_end) { 
      goto lbl_svr_loop;
    };      
  } 
}
*)
\* BEGIN TRANSLATION
\* Process variable next_txn of process Coo at line 829 col 13 changed to next_txn_
\* Process variable next_pie of process Coo at line 830 col 13 changed to next_pie_
\* Process variable pre_accept_acks of process Coo at line 835 col 13 changed to pre_accept_acks_
\* Process variable msg_in of process Coo at line 844 col 13 changed to msg_in_
\* Process variable msg_out of process Coo at line 845 col 13 changed to msg_out_
\* Process variable curr_pie of process Svr at line 1010 col 13 changed to curr_pie_
\* Parameter pre_accept_acks of procedure AggregateGraphForFastPath at line 581 col 39 changed to pre_accept_acks_A
\* Parameter partitions of procedure AggregateGraphForFastPath at line 581 col 56 changed to partitions_
CONSTANT defaultInitValue
VARIABLES coo_mq, svr_mq, srz, n_committed, id_counter, pc, stack, 
          pre_accept_acks_A, partitions_, pre_accept_acks, partitions, 
          curr_pie, next_txn_, next_pie_, txn_id, dep_c, par, n_pie, 
          pre_accept_acks_, accept_acks, committed, ballot, tmp_ack, tmp_acks, 
          tmp_par, tmp_partitions, shards, msg_in_, msg_out_, keyspace, dep, 
          ya_dep, finished_map, asked_map, pie_map, tx_meta, scc, scc_set, 
          scc_seq, curr_txn, curr_pie_, curr_op, txn_set, tid_set, opset, 
          next_txn, next_tid, next_pie, anc, tmp_set, alogs, vid_set, 
          sub_graph, tx_meta_set, msg_in, msg_out, svr_end

vars == << coo_mq, svr_mq, srz, n_committed, id_counter, pc, stack, 
           pre_accept_acks_A, partitions_, pre_accept_acks, partitions, 
           curr_pie, next_txn_, next_pie_, txn_id, dep_c, par, n_pie, 
           pre_accept_acks_, accept_acks, committed, ballot, tmp_ack, 
           tmp_acks, tmp_par, tmp_partitions, shards, msg_in_, msg_out_, 
           keyspace, dep, ya_dep, finished_map, asked_map, pie_map, tx_meta, 
           scc, scc_set, scc_seq, curr_txn, curr_pie_, curr_op, txn_set, 
           tid_set, opset, next_txn, next_tid, next_pie, anc, tmp_set, alogs, 
           vid_set, sub_graph, tx_meta_set, msg_in, msg_out, svr_end >>

ProcSet == (1 .. M) \cup (M+1 .. M+N*X)

Init == (* Global variables *)
        /\ coo_mq = [i \in 1 .. M |-> <<>>]
        /\ svr_mq = [i \in M .. M+N*X |-> <<>>]
        /\ srz = EmptySrzGraph
        /\ n_committed = 0
        /\ id_counter = 0
        (* Procedure AggregateGraphForFastPath *)
        /\ pre_accept_acks_A = [ self \in ProcSet |-> defaultInitValue]
        /\ partitions_ = [ self \in ProcSet |-> defaultInitValue]
        (* Procedure AggregateGraphForAccept *)
        /\ pre_accept_acks = [ self \in ProcSet |-> defaultInitValue]
        /\ partitions = [ self \in ProcSet |-> defaultInitValue]
        (* Procedure ExePie *)
        /\ curr_pie = [ self \in ProcSet |-> defaultInitValue]
        (* Process Coo *)
        /\ next_txn_ = [self \in 1 .. M |-> defaultInitValue]
        /\ next_pie_ = [self \in 1 .. M |-> defaultInitValue]
        /\ txn_id = [self \in 1 .. M |-> defaultInitValue]
        /\ dep_c = [self \in 1 .. M |-> defaultInitValue]
        /\ par = [self \in 1 .. M |-> defaultInitValue]
        /\ n_pie = [self \in 1 .. M |-> defaultInitValue]
        /\ pre_accept_acks_ = [self \in 1 .. M |-> defaultInitValue]
        /\ accept_acks = [self \in 1 .. M |-> defaultInitValue]
        /\ committed = [self \in 1 .. M |-> defaultInitValue]
        /\ ballot = [self \in 1 .. M |-> defaultInitValue]
        /\ tmp_ack = [self \in 1 .. M |-> defaultInitValue]
        /\ tmp_acks = [self \in 1 .. M |-> defaultInitValue]
        /\ tmp_par = [self \in 1 .. M |-> defaultInitValue]
        /\ tmp_partitions = [self \in 1 .. M |-> defaultInitValue]
        /\ shards = [self \in 1 .. M |-> defaultInitValue]
        /\ msg_in_ = [self \in 1 .. M |-> defaultInitValue]
        /\ msg_out_ = [self \in 1 .. M |-> defaultInitValue]
        (* Process Svr *)
        /\ keyspace = [self \in M+1 .. M+N*X |-> DEFAULT_KEY_SPACE]
        /\ dep = [self \in M+1 .. M+N*X |-> EmptyNewDep]
        /\ ya_dep = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ finished_map = [self \in M+1 .. M+N*X |-> [i \in 1..M |-> FALSE]]
        /\ asked_map = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ pie_map = [self \in M+1 .. M+N*X |-> [i \in 1..M |-> {}]]
        /\ tx_meta = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ scc = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ scc_set = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ scc_seq = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ curr_txn = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ curr_pie_ = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ curr_op = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ txn_set = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ tid_set = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ opset = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ next_txn = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ next_tid = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ next_pie = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ anc = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ tmp_set = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ alogs = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ vid_set = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ sub_graph = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ tx_meta_set = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ msg_in = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ msg_out = [self \in M+1 .. M+N*X |-> defaultInitValue]
        /\ svr_end = [self \in M+1 .. M+N*X |-> FALSE]
        /\ stack = [self \in ProcSet |-> << >>]
        /\ pc = [self \in ProcSet |-> CASE self \in 1 .. M -> "lbl_coord_begin"
                                        [] self \in M+1 .. M+N*X -> "lbl_svr_loop"]

__debug_label_kldjf(self) == /\ pc[self] = "__debug_label_kldjf"
                             /\ Assert((SrzAcyclic(EmptySrzGraph)), 
                                       "Failure of assertion at line 568, column 26.")
                             /\ pc' = [pc EXCEPT ![self] = "test_label_1"]
                             /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                             id_counter, stack, 
                                             pre_accept_acks_A, partitions_, 
                                             pre_accept_acks, partitions, 
                                             curr_pie, next_txn_, next_pie_, 
                                             txn_id, dep_c, par, n_pie, 
                                             pre_accept_acks_, accept_acks, 
                                             committed, ballot, tmp_ack, 
                                             tmp_acks, tmp_par, tmp_partitions, 
                                             shards, msg_in_, msg_out_, 
                                             keyspace, dep, ya_dep, 
                                             finished_map, asked_map, pie_map, 
                                             tx_meta, scc, scc_set, scc_seq, 
                                             curr_txn, curr_pie_, curr_op, 
                                             txn_set, tid_set, opset, next_txn, 
                                             next_tid, next_pie, anc, tmp_set, 
                                             alogs, vid_set, sub_graph, 
                                             tx_meta_set, msg_in, msg_out, 
                                             svr_end >>

test_label_1(self) == /\ pc[self] = "test_label_1"
                      /\ tmp_partitions' = [tmp_partitions EXCEPT ![self] = PartitionsToProcIds({1})]
                      /\ IF M = 1 /\ X = 1
                            THEN /\ pc' = [pc EXCEPT ![self] = "debug_label_dslfkj"]
                            ELSE /\ IF M = 1 /\ X = 2
                                       THEN /\ pc' = [pc EXCEPT ![self] = "debug_label_lklfkj"]
                                       ELSE /\ pc' = [pc EXCEPT ![self] = "lbl_dbg_dksfjlas"]
                      /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                      id_counter, stack, pre_accept_acks_A, 
                                      partitions_, pre_accept_acks, partitions, 
                                      curr_pie, next_txn_, next_pie_, txn_id, 
                                      dep_c, par, n_pie, pre_accept_acks_, 
                                      accept_acks, committed, ballot, tmp_ack, 
                                      tmp_acks, tmp_par, shards, msg_in_, 
                                      msg_out_, keyspace, dep, ya_dep, 
                                      finished_map, asked_map, pie_map, 
                                      tx_meta, scc, scc_set, scc_seq, curr_txn, 
                                      curr_pie_, curr_op, txn_set, tid_set, 
                                      opset, next_txn, next_tid, next_pie, anc, 
                                      tmp_set, alogs, vid_set, sub_graph, 
                                      tx_meta_set, msg_in, msg_out, svr_end >>

debug_label_dslfkj(self) == /\ pc[self] = "debug_label_dslfkj"
                            /\ Assert((tmp_partitions[self] = {2}), 
                                      "Failure of assertion at line 571, column 27.")
                            /\ pc' = [pc EXCEPT ![self] = "lbl_dbg_dksfjlas"]
                            /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                            id_counter, stack, 
                                            pre_accept_acks_A, partitions_, 
                                            pre_accept_acks, partitions, 
                                            curr_pie, next_txn_, next_pie_, 
                                            txn_id, dep_c, par, n_pie, 
                                            pre_accept_acks_, accept_acks, 
                                            committed, ballot, tmp_ack, 
                                            tmp_acks, tmp_par, tmp_partitions, 
                                            shards, msg_in_, msg_out_, 
                                            keyspace, dep, ya_dep, 
                                            finished_map, asked_map, pie_map, 
                                            tx_meta, scc, scc_set, scc_seq, 
                                            curr_txn, curr_pie_, curr_op, 
                                            txn_set, tid_set, opset, next_txn, 
                                            next_tid, next_pie, anc, tmp_set, 
                                            alogs, vid_set, sub_graph, 
                                            tx_meta_set, msg_in, msg_out, 
                                            svr_end >>

debug_label_lklfkj(self) == /\ pc[self] = "debug_label_lklfkj"
                            /\ Assert((tmp_partitions[self] = {2,3}), 
                                      "Failure of assertion at line 573, column 27.")
                            /\ pc' = [pc EXCEPT ![self] = "lbl_dbg_dksfjlas"]
                            /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                            id_counter, stack, 
                                            pre_accept_acks_A, partitions_, 
                                            pre_accept_acks, partitions, 
                                            curr_pie, next_txn_, next_pie_, 
                                            txn_id, dep_c, par, n_pie, 
                                            pre_accept_acks_, accept_acks, 
                                            committed, ballot, tmp_ack, 
                                            tmp_acks, tmp_par, tmp_partitions, 
                                            shards, msg_in_, msg_out_, 
                                            keyspace, dep, ya_dep, 
                                            finished_map, asked_map, pie_map, 
                                            tx_meta, scc, scc_set, scc_seq, 
                                            curr_txn, curr_pie_, curr_op, 
                                            txn_set, tid_set, opset, next_txn, 
                                            next_tid, next_pie, anc, tmp_set, 
                                            alogs, vid_set, sub_graph, 
                                            tx_meta_set, msg_in, msg_out, 
                                            svr_end >>

lbl_dbg_dksfjlas(self) == /\ pc[self] = "lbl_dbg_dksfjlas"
                          /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                          /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                          /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                          id_counter, pre_accept_acks_A, 
                                          partitions_, pre_accept_acks, 
                                          partitions, curr_pie, next_txn_, 
                                          next_pie_, txn_id, dep_c, par, n_pie, 
                                          pre_accept_acks_, accept_acks, 
                                          committed, ballot, tmp_ack, tmp_acks, 
                                          tmp_par, tmp_partitions, shards, 
                                          msg_in_, msg_out_, keyspace, dep, 
                                          ya_dep, finished_map, asked_map, 
                                          pie_map, tx_meta, scc, scc_set, 
                                          scc_seq, curr_txn, curr_pie_, 
                                          curr_op, txn_set, tid_set, opset, 
                                          next_txn, next_tid, next_pie, anc, 
                                          tmp_set, alogs, vid_set, sub_graph, 
                                          tx_meta_set, msg_in, msg_out, 
                                          svr_end >>

UnitTest(self) == __debug_label_kldjf(self) \/ test_label_1(self)
                     \/ debug_label_dslfkj(self)
                     \/ debug_label_lklfkj(self) \/ lbl_dbg_dksfjlas(self)

xxxxdddx(self) == /\ pc[self] = "xxxxdddx"
                  /\ tmp_partitions' = [tmp_partitions EXCEPT ![self] = partitions_[self]]
                  /\ pc' = [pc EXCEPT ![self] = "yyyyassy"]
                  /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, id_counter, 
                                  stack, pre_accept_acks_A, partitions_, 
                                  pre_accept_acks, partitions, curr_pie, 
                                  next_txn_, next_pie_, txn_id, dep_c, par, 
                                  n_pie, pre_accept_acks_, accept_acks, 
                                  committed, ballot, tmp_ack, tmp_acks, 
                                  tmp_par, shards, msg_in_, msg_out_, keyspace, 
                                  dep, ya_dep, finished_map, asked_map, 
                                  pie_map, tx_meta, scc, scc_set, scc_seq, 
                                  curr_txn, curr_pie_, curr_op, txn_set, 
                                  tid_set, opset, next_txn, next_tid, next_pie, 
                                  anc, tmp_set, alogs, vid_set, sub_graph, 
                                  tx_meta_set, msg_in, msg_out, svr_end >>

yyyyassy(self) == /\ pc[self] = "yyyyassy"
                  /\ IF tmp_partitions[self] /= {}
                        THEN /\ \E p \in tmp_partitions[self]:
                                  /\ tmp_par' = [tmp_par EXCEPT ![self] = p]
                                  /\ tmp_partitions' = [tmp_partitions EXCEPT ![self] = @ \ {p}]
                             /\ tmp_ack' = [tmp_ack EXCEPT ![self] = PickXXX(pre_accept_acks_A[self][tmp_par'[self]])]
                             /\ dep_c' = [dep_c EXCEPT ![self] = @ \union tmp_ack'[self].dep]
                             /\ pc' = [pc EXCEPT ![self] = "yyyyassy"]
                             /\ UNCHANGED << stack, pre_accept_acks_A, 
                                             partitions_ >>
                        ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                             /\ pre_accept_acks_A' = [pre_accept_acks_A EXCEPT ![self] = Head(stack[self]).pre_accept_acks_A]
                             /\ partitions_' = [partitions_ EXCEPT ![self] = Head(stack[self]).partitions_]
                             /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                             /\ UNCHANGED << dep_c, tmp_ack, tmp_par, 
                                             tmp_partitions >>
                  /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, id_counter, 
                                  pre_accept_acks, partitions, curr_pie, 
                                  next_txn_, next_pie_, txn_id, par, n_pie, 
                                  pre_accept_acks_, accept_acks, committed, 
                                  ballot, tmp_acks, shards, msg_in_, msg_out_, 
                                  keyspace, dep, ya_dep, finished_map, 
                                  asked_map, pie_map, tx_meta, scc, scc_set, 
                                  scc_seq, curr_txn, curr_pie_, curr_op, 
                                  txn_set, tid_set, opset, next_txn, next_tid, 
                                  next_pie, anc, tmp_set, alogs, vid_set, 
                                  sub_graph, tx_meta_set, msg_in, msg_out, 
                                  svr_end >>

AggregateGraphForFastPath(self) == xxxxdddx(self) \/ yyyyassy(self)

slowpath_init(self) == /\ pc[self] = "slowpath_init"
                       /\ tmp_partitions' = [tmp_partitions EXCEPT ![self] = partitions[self]]
                       /\ pc' = [pc EXCEPT ![self] = "slowpath_scan_acks"]
                       /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                       id_counter, stack, pre_accept_acks_A, 
                                       partitions_, pre_accept_acks, 
                                       partitions, curr_pie, next_txn_, 
                                       next_pie_, txn_id, dep_c, par, n_pie, 
                                       pre_accept_acks_, accept_acks, 
                                       committed, ballot, tmp_ack, tmp_acks, 
                                       tmp_par, shards, msg_in_, msg_out_, 
                                       keyspace, dep, ya_dep, finished_map, 
                                       asked_map, pie_map, tx_meta, scc, 
                                       scc_set, scc_seq, curr_txn, curr_pie_, 
                                       curr_op, txn_set, tid_set, opset, 
                                       next_txn, next_tid, next_pie, anc, 
                                       tmp_set, alogs, vid_set, sub_graph, 
                                       tx_meta_set, msg_in, msg_out, svr_end >>

slowpath_scan_acks(self) == /\ pc[self] = "slowpath_scan_acks"
                            /\ IF tmp_partitions[self] /= {}
                                  THEN /\ \E p \in tmp_partitions[self]:
                                            /\ tmp_acks' = [tmp_acks EXCEPT ![self] = pre_accept_acks[self][p]]
                                            /\ tmp_partitions' = [tmp_partitions EXCEPT ![self] = @ \ {p}]
                                       /\ pc' = [pc EXCEPT ![self] = "zzzzz"]
                                       /\ UNCHANGED << stack, pre_accept_acks, 
                                                       partitions >>
                                  ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                       /\ pre_accept_acks' = [pre_accept_acks EXCEPT ![self] = Head(stack[self]).pre_accept_acks]
                                       /\ partitions' = [partitions EXCEPT ![self] = Head(stack[self]).partitions]
                                       /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                       /\ UNCHANGED << tmp_acks, 
                                                       tmp_partitions >>
                            /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                            id_counter, pre_accept_acks_A, 
                                            partitions_, curr_pie, next_txn_, 
                                            next_pie_, txn_id, dep_c, par, 
                                            n_pie, pre_accept_acks_, 
                                            accept_acks, committed, ballot, 
                                            tmp_ack, tmp_par, shards, msg_in_, 
                                            msg_out_, keyspace, dep, ya_dep, 
                                            finished_map, asked_map, pie_map, 
                                            tx_meta, scc, scc_set, scc_seq, 
                                            curr_txn, curr_pie_, curr_op, 
                                            txn_set, tid_set, opset, next_txn, 
                                            next_tid, next_pie, anc, tmp_set, 
                                            alogs, vid_set, sub_graph, 
                                            tx_meta_set, msg_in, msg_out, 
                                            svr_end >>

zzzzz(self) == /\ pc[self] = "zzzzz"
               /\ IF tmp_acks[self] /= {}
                     THEN /\ \E a \in tmp_acks[self]:
                               /\ dep_c' = [dep_c EXCEPT ![self] = @ \union a.dep]
                               /\ tmp_acks' = [tmp_acks EXCEPT ![self] = @ \ {a}]
                          /\ pc' = [pc EXCEPT ![self] = "zzzzz"]
                     ELSE /\ pc' = [pc EXCEPT ![self] = "slowpath_scan_acks"]
                          /\ UNCHANGED << dep_c, tmp_acks >>
               /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, id_counter, 
                               stack, pre_accept_acks_A, partitions_, 
                               pre_accept_acks, partitions, curr_pie, 
                               next_txn_, next_pie_, txn_id, par, n_pie, 
                               pre_accept_acks_, accept_acks, committed, 
                               ballot, tmp_ack, tmp_par, tmp_partitions, 
                               shards, msg_in_, msg_out_, keyspace, dep, 
                               ya_dep, finished_map, asked_map, pie_map, 
                               tx_meta, scc, scc_set, scc_seq, curr_txn, 
                               curr_pie_, curr_op, txn_set, tid_set, opset, 
                               next_txn, next_tid, next_pie, anc, tmp_set, 
                               alogs, vid_set, sub_graph, tx_meta_set, msg_in, 
                               msg_out, svr_end >>

AggregateGraphForAccept(self) == slowpath_init(self)
                                    \/ slowpath_scan_acks(self)
                                    \/ zzzzz(self)

lbl_on_pre_accept(self) == /\ pc[self] = "lbl_on_pre_accept"
                           /\ curr_txn' = [curr_txn EXCEPT ![self].status = PRE_ACCEPTED]
                           /\ curr_pie' = [curr_pie EXCEPT ![self] = msg_in[self].pie]
                           /\ pie_map' = [pie_map EXCEPT ![self][msg_in[self].tid] = @ \union {msg_in[self].pie}]
                           /\ opset' = [opset EXCEPT ![self] = curr_pie'[self].opset]
                           /\ Assert((opset'[self] /= {}), 
                                     "Failure of assertion at line 620, column 5.")
                           /\ pc' = [pc EXCEPT ![self] = "lbl_pie_add_dep"]
                           /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                           id_counter, stack, 
                                           pre_accept_acks_A, partitions_, 
                                           pre_accept_acks, partitions, 
                                           next_txn_, next_pie_, txn_id, dep_c, 
                                           par, n_pie, pre_accept_acks_, 
                                           accept_acks, committed, ballot, 
                                           tmp_ack, tmp_acks, tmp_par, 
                                           tmp_partitions, shards, msg_in_, 
                                           msg_out_, keyspace, dep, ya_dep, 
                                           finished_map, asked_map, tx_meta, 
                                           scc, scc_set, scc_seq, curr_pie_, 
                                           curr_op, txn_set, tid_set, next_txn, 
                                           next_tid, next_pie, anc, tmp_set, 
                                           alogs, vid_set, sub_graph, 
                                           tx_meta_set, msg_in, msg_out, 
                                           svr_end >>

lbl_pie_add_dep(self) == /\ pc[self] = "lbl_pie_add_dep"
                         /\ IF opset[self] /= {}
                               THEN /\ \E next_op \in opset[self]:
                                         /\ opset' = [opset EXCEPT ![self] = opset[self] \ {next_op}]
                                         /\ curr_op' = [curr_op EXCEPT ![self] = next_op]
                                    /\ tx_meta' = [tx_meta EXCEPT ![self] = keyspace[self][curr_op'[self].key].last_tx_meta]
                                    /\ IF tx_meta'[self].id /= 0
                                          THEN /\ curr_txn' = [curr_txn EXCEPT ![self].parents = @ \union {tx_meta'[self]}]
                                          ELSE /\ TRUE
                                               /\ UNCHANGED curr_txn
                                    /\ keyspace' = [keyspace EXCEPT ![self][curr_op'[self].key].last_tx_meta = curr_txn'[self].meta]
                                    /\ pc' = [pc EXCEPT ![self] = "lbl_pie_add_dep"]
                                    /\ UNCHANGED << coo_mq, stack, dep, 
                                                    msg_out >>
                               ELSE /\ curr_txn' = [curr_txn EXCEPT ![self].partitions = {ProcIdToPartitionId(self)}]
                                    /\ dep' = [dep EXCEPT ![self] = NewDepUpdateVertex(dep[self], curr_txn'[self])]
                                    /\ msg_out' = [msg_out EXCEPT ![self] = [type |-> "ack_pre_accept",
                                                                             tid |-> curr_txn'[self].tid,
                                                                             src |-> self,
                                                                             dep |-> curr_txn'[self].parents]]
                                    /\ coo_mq' = [coo_mq EXCEPT ![msg_in[self].src] = Append(coo_mq[msg_in[self].src], msg_out'[self])]
                                    /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                    /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                    /\ UNCHANGED << keyspace, tx_meta, curr_op, 
                                                    opset >>
                         /\ UNCHANGED << svr_mq, srz, n_committed, id_counter, 
                                         pre_accept_acks_A, partitions_, 
                                         pre_accept_acks, partitions, curr_pie, 
                                         next_txn_, next_pie_, txn_id, dep_c, 
                                         par, n_pie, pre_accept_acks_, 
                                         accept_acks, committed, ballot, 
                                         tmp_ack, tmp_acks, tmp_par, 
                                         tmp_partitions, shards, msg_in_, 
                                         msg_out_, ya_dep, finished_map, 
                                         asked_map, pie_map, scc, scc_set, 
                                         scc_seq, curr_pie_, txn_set, tid_set, 
                                         next_txn, next_tid, next_pie, anc, 
                                         tmp_set, alogs, vid_set, sub_graph, 
                                         tx_meta_set, msg_in, svr_end >>

HandlePreAccept(self) == lbl_on_pre_accept(self) \/ lbl_pie_add_dep(self)

lbl_on_prepare(self) == /\ pc[self] = "lbl_on_prepare"
                        /\ dep' = [dep EXCEPT ![self] = UpdateBallot(dep[self], curr_txn[self].tid, msg_in[self].ballot, PREPARED)]
                        /\ msg_out' = [msg_out EXCEPT ![self] = [type |-> "ack_prepare", src |-> self, tid |-> msg_in[self].tid, ballot |-> msg_in[self].ballot]]
                        /\ pc' = [pc EXCEPT ![self] = "lbl_on_prep_reply"]
                        /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                        id_counter, stack, pre_accept_acks_A, 
                                        partitions_, pre_accept_acks, 
                                        partitions, curr_pie, next_txn_, 
                                        next_pie_, txn_id, dep_c, par, n_pie, 
                                        pre_accept_acks_, accept_acks, 
                                        committed, ballot, tmp_ack, tmp_acks, 
                                        tmp_par, tmp_partitions, shards, 
                                        msg_in_, msg_out_, keyspace, ya_dep, 
                                        finished_map, asked_map, pie_map, 
                                        tx_meta, scc, scc_set, scc_seq, 
                                        curr_txn, curr_pie_, curr_op, txn_set, 
                                        tid_set, opset, next_txn, next_tid, 
                                        next_pie, anc, tmp_set, alogs, vid_set, 
                                        sub_graph, tx_meta_set, msg_in, 
                                        svr_end >>

lbl_on_prep_reply(self) == /\ pc[self] = "lbl_on_prep_reply"
                           /\ svr_mq' = [svr_mq EXCEPT ![msg_in[self].src] = Append(coo_mq[msg_in[self].src], msg_out[self])]
                           /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                           /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                           /\ UNCHANGED << coo_mq, srz, n_committed, 
                                           id_counter, pre_accept_acks_A, 
                                           partitions_, pre_accept_acks, 
                                           partitions, curr_pie, next_txn_, 
                                           next_pie_, txn_id, dep_c, par, 
                                           n_pie, pre_accept_acks_, 
                                           accept_acks, committed, ballot, 
                                           tmp_ack, tmp_acks, tmp_par, 
                                           tmp_partitions, shards, msg_in_, 
                                           msg_out_, keyspace, dep, ya_dep, 
                                           finished_map, asked_map, pie_map, 
                                           tx_meta, scc, scc_set, scc_seq, 
                                           curr_txn, curr_pie_, curr_op, 
                                           txn_set, tid_set, opset, next_txn, 
                                           next_tid, next_pie, anc, tmp_set, 
                                           alogs, vid_set, sub_graph, 
                                           tx_meta_set, msg_in, msg_out, 
                                           svr_end >>

HandlePrepare(self) == lbl_on_prepare(self) \/ lbl_on_prep_reply(self)

lbl_on_accept_1(self) == /\ pc[self] = "lbl_on_accept_1"
                         /\ dep' = [dep EXCEPT ![self][msg_in[self].tid].status = ACCEPTED]
                         /\ pc' = [pc EXCEPT ![self] = "lbl_on_accept_2"]
                         /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                         id_counter, stack, pre_accept_acks_A, 
                                         partitions_, pre_accept_acks, 
                                         partitions, curr_pie, next_txn_, 
                                         next_pie_, txn_id, dep_c, par, n_pie, 
                                         pre_accept_acks_, accept_acks, 
                                         committed, ballot, tmp_ack, tmp_acks, 
                                         tmp_par, tmp_partitions, shards, 
                                         msg_in_, msg_out_, keyspace, ya_dep, 
                                         finished_map, asked_map, pie_map, 
                                         tx_meta, scc, scc_set, scc_seq, 
                                         curr_txn, curr_pie_, curr_op, txn_set, 
                                         tid_set, opset, next_txn, next_tid, 
                                         next_pie, anc, tmp_set, alogs, 
                                         vid_set, sub_graph, tx_meta_set, 
                                         msg_in, msg_out, svr_end >>

lbl_on_accept_2(self) == /\ pc[self] = "lbl_on_accept_2"
                         /\ dep' = [dep EXCEPT ![self][msg_in[self].tid].parents = msg_in[self].dep]
                         /\ msg_out' = [msg_out EXCEPT ![self] = [type |-> "ack_accept", src |-> self, tid |-> msg_in[self].tid, ballot |-> msg_in[self].ballot]]
                         /\ coo_mq' = [coo_mq EXCEPT ![msg_in[self].src] = Append(coo_mq[msg_in[self].src], msg_out'[self])]
                         /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                         /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                         /\ UNCHANGED << svr_mq, srz, n_committed, id_counter, 
                                         pre_accept_acks_A, partitions_, 
                                         pre_accept_acks, partitions, curr_pie, 
                                         next_txn_, next_pie_, txn_id, dep_c, 
                                         par, n_pie, pre_accept_acks_, 
                                         accept_acks, committed, ballot, 
                                         tmp_ack, tmp_acks, tmp_par, 
                                         tmp_partitions, shards, msg_in_, 
                                         msg_out_, keyspace, ya_dep, 
                                         finished_map, asked_map, pie_map, 
                                         tx_meta, scc, scc_set, scc_seq, 
                                         curr_txn, curr_pie_, curr_op, txn_set, 
                                         tid_set, opset, next_txn, next_tid, 
                                         next_pie, anc, tmp_set, alogs, 
                                         vid_set, sub_graph, tx_meta_set, 
                                         msg_in, svr_end >>

HandleAccept(self) == lbl_on_accept_1(self) \/ lbl_on_accept_2(self)

lbl_inquire_unknown(self) == /\ pc[self] = "lbl_inquire_unknown"
                             /\ tx_meta_set' = [tx_meta_set EXCEPT ![self] = msg_in[self].dep]
                             /\ pc' = [pc EXCEPT ![self] = "lbl_at_inquire"]
                             /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                             id_counter, stack, 
                                             pre_accept_acks_A, partitions_, 
                                             pre_accept_acks, partitions, 
                                             curr_pie, next_txn_, next_pie_, 
                                             txn_id, dep_c, par, n_pie, 
                                             pre_accept_acks_, accept_acks, 
                                             committed, ballot, tmp_ack, 
                                             tmp_acks, tmp_par, tmp_partitions, 
                                             shards, msg_in_, msg_out_, 
                                             keyspace, dep, ya_dep, 
                                             finished_map, asked_map, pie_map, 
                                             tx_meta, scc, scc_set, scc_seq, 
                                             curr_txn, curr_pie_, curr_op, 
                                             txn_set, tid_set, opset, next_txn, 
                                             next_tid, next_pie, anc, tmp_set, 
                                             alogs, vid_set, sub_graph, msg_in, 
                                             msg_out, svr_end >>

lbl_at_inquire(self) == /\ pc[self] = "lbl_at_inquire"
                        /\ IF tx_meta_set[self] /= {}
                              THEN /\ \E meta \in tx_meta_set[self]:
                                        /\ Assert((meta.id > 0), 
                                                  "Failure of assertion at line 672, column 9.")
                                        /\ tx_meta_set' = [tx_meta_set EXCEPT ![self] = @ \ {meta}]
                                        /\ IF ProcIdToPartitionId(self) \in meta.shards
                                              THEN /\ TRUE
                                                   /\ UNCHANGED << svr_mq, 
                                                                   msg_out >>
                                              ELSE /\ Assert((FALSE), 
                                                             "Failure of assertion at line 677, column 11.")
                                                   /\ \E dst \in meta.shards:
                                                        /\ msg_out' = [msg_out EXCEPT ![self] = [type |-> "inquire", tid |-> meta.id, src |-> self]]
                                                        /\ svr_mq' = BroadcastMsg(svr_mq, dst, msg_out'[self])
                                                        /\ Assert((FALSE), 
                                                                  "Failure of assertion at line 681, column 13.")
                                                        /\ TRUE
                                   /\ pc' = [pc EXCEPT ![self] = "lbl_at_inquire"]
                                   /\ stack' = stack
                              ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                   /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                   /\ UNCHANGED << svr_mq, tx_meta_set, 
                                                   msg_out >>
                        /\ UNCHANGED << coo_mq, srz, n_committed, id_counter, 
                                        pre_accept_acks_A, partitions_, 
                                        pre_accept_acks, partitions, curr_pie, 
                                        next_txn_, next_pie_, txn_id, dep_c, 
                                        par, n_pie, pre_accept_acks_, 
                                        accept_acks, committed, ballot, 
                                        tmp_ack, tmp_acks, tmp_par, 
                                        tmp_partitions, shards, msg_in_, 
                                        msg_out_, keyspace, dep, ya_dep, 
                                        finished_map, asked_map, pie_map, 
                                        tx_meta, scc, scc_set, scc_seq, 
                                        curr_txn, curr_pie_, curr_op, txn_set, 
                                        tid_set, opset, next_txn, next_tid, 
                                        next_pie, anc, tmp_set, alogs, vid_set, 
                                        sub_graph, msg_in, svr_end >>

InquireUnknown(self) == lbl_inquire_unknown(self) \/ lbl_at_inquire(self)

lbl_exe_scc_set(self) == /\ pc[self] = "lbl_exe_scc_set"
                         /\ vid_set' = [vid_set EXCEPT ![self] = scc[self]]
                         /\ Assert((vid_set'[self] /= {}), 
                                   "Failure of assertion at line 693, column 5.")
                         /\ pc' = [pc EXCEPT ![self] = "lbl_exe_scc"]
                         /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                         id_counter, stack, pre_accept_acks_A, 
                                         partitions_, pre_accept_acks, 
                                         partitions, curr_pie, next_txn_, 
                                         next_pie_, txn_id, dep_c, par, n_pie, 
                                         pre_accept_acks_, accept_acks, 
                                         committed, ballot, tmp_ack, tmp_acks, 
                                         tmp_par, tmp_partitions, shards, 
                                         msg_in_, msg_out_, keyspace, dep, 
                                         ya_dep, finished_map, asked_map, 
                                         pie_map, tx_meta, scc, scc_set, 
                                         scc_seq, curr_txn, curr_pie_, curr_op, 
                                         txn_set, tid_set, opset, next_txn, 
                                         next_tid, next_pie, anc, tmp_set, 
                                         alogs, sub_graph, tx_meta_set, msg_in, 
                                         msg_out, svr_end >>

lbl_exe_scc(self) == /\ pc[self] = "lbl_exe_scc"
                     /\ IF vid_set[self] /= {}
                           THEN /\ next_tid' = [next_tid EXCEPT ![self] = MinVid(scc[self])]
                                /\ vid_set' = [vid_set EXCEPT ![self] = vid_set[self] \ {next_tid'[self]}]
                                /\ IF /\ IsInvolved(dep[self], next_tid'[self], ProcIdToPartitionId(self))
                                      /\ Unfinished(finished_map[self], next_tid'[self])
                                      THEN /\ pc' = [pc EXCEPT ![self] = "__debug_label_zcxvx"]
                                      ELSE /\ IF M=1
                                                 THEN /\ Assert((FALSE), 
                                                                "Failure of assertion at line 724, column 11.")
                                                 ELSE /\ TRUE
                                           /\ pc' = [pc EXCEPT ![self] = "lbl_exe_scc"]
                                /\ stack' = stack
                           ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                /\ UNCHANGED << next_tid, vid_set >>
                     /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                     id_counter, pre_accept_acks_A, 
                                     partitions_, pre_accept_acks, partitions, 
                                     curr_pie, next_txn_, next_pie_, txn_id, 
                                     dep_c, par, n_pie, pre_accept_acks_, 
                                     accept_acks, committed, ballot, tmp_ack, 
                                     tmp_acks, tmp_par, tmp_partitions, shards, 
                                     msg_in_, msg_out_, keyspace, dep, ya_dep, 
                                     finished_map, asked_map, pie_map, tx_meta, 
                                     scc, scc_set, scc_seq, curr_txn, 
                                     curr_pie_, curr_op, txn_set, tid_set, 
                                     opset, next_txn, next_pie, anc, tmp_set, 
                                     alogs, sub_graph, tx_meta_set, msg_in, 
                                     msg_out, svr_end >>

__debug_label_zcxvx(self) == /\ pc[self] = "__debug_label_zcxvx"
                             /\ Assert((pie_map[self][next_tid[self]] /= {}), 
                                       "Failure of assertion at line 701, column 30.")
                             /\ IF M = 1
                                   THEN /\ pc' = [pc EXCEPT ![self] = "__debug_label_kdjf"]
                                   ELSE /\ pc' = [pc EXCEPT ![self] = "exe_all_deferred_pies"]
                             /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                             id_counter, stack, 
                                             pre_accept_acks_A, partitions_, 
                                             pre_accept_acks, partitions, 
                                             curr_pie, next_txn_, next_pie_, 
                                             txn_id, dep_c, par, n_pie, 
                                             pre_accept_acks_, accept_acks, 
                                             committed, ballot, tmp_ack, 
                                             tmp_acks, tmp_par, tmp_partitions, 
                                             shards, msg_in_, msg_out_, 
                                             keyspace, dep, ya_dep, 
                                             finished_map, asked_map, pie_map, 
                                             tx_meta, scc, scc_set, scc_seq, 
                                             curr_txn, curr_pie_, curr_op, 
                                             txn_set, tid_set, opset, next_txn, 
                                             next_tid, next_pie, anc, tmp_set, 
                                             alogs, vid_set, sub_graph, 
                                             tx_meta_set, msg_in, msg_out, 
                                             svr_end >>

__debug_label_kdjf(self) == /\ pc[self] = "__debug_label_kdjf"
                            /\ Assert((next_tid[self] = 1), 
                                      "Failure of assertion at line 703, column 31.")
                            /\ pc' = [pc EXCEPT ![self] = "exe_all_deferred_pies"]
                            /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                            id_counter, stack, 
                                            pre_accept_acks_A, partitions_, 
                                            pre_accept_acks, partitions, 
                                            curr_pie, next_txn_, next_pie_, 
                                            txn_id, dep_c, par, n_pie, 
                                            pre_accept_acks_, accept_acks, 
                                            committed, ballot, tmp_ack, 
                                            tmp_acks, tmp_par, tmp_partitions, 
                                            shards, msg_in_, msg_out_, 
                                            keyspace, dep, ya_dep, 
                                            finished_map, asked_map, pie_map, 
                                            tx_meta, scc, scc_set, scc_seq, 
                                            curr_txn, curr_pie_, curr_op, 
                                            txn_set, tid_set, opset, next_txn, 
                                            next_tid, next_pie, anc, tmp_set, 
                                            alogs, vid_set, sub_graph, 
                                            tx_meta_set, msg_in, msg_out, 
                                            svr_end >>

exe_all_deferred_pies(self) == /\ pc[self] = "exe_all_deferred_pies"
                               /\ IF pie_map[self][next_tid[self]] /= {}
                                     THEN /\ \E pie \in pie_map[self][next_tid[self]]:
                                               /\ next_pie' = [next_pie EXCEPT ![self] = pie]
                                               /\ pie_map' = [pie_map EXCEPT ![self][next_tid[self]] = @ \ {next_pie'[self]}]
                                          /\ /\ curr_pie' = [curr_pie EXCEPT ![self] = next_pie'[self]]
                                             /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "ExePie",
                                                                                      pc        |->  "exe_all_deferred_pies",
                                                                                      curr_pie  |->  curr_pie[self] ] >>
                                                                                  \o stack[self]]
                                          /\ pc' = [pc EXCEPT ![self] = "lbl_exe_pie"]
                                          /\ UNCHANGED << coo_mq, finished_map, 
                                                          msg_out >>
                                     ELSE /\ finished_map' = [finished_map EXCEPT ![self][next_tid[self]] = TRUE]
                                          /\ msg_out' = [msg_out EXCEPT ![self] = [type |-> "ack_commit",
                                                                                   tid |-> next_tid[self],
                                                                                   src |-> self]]
                                          /\ coo_mq' = [coo_mq EXCEPT ![next_tid[self]] = Append(@, msg_out'[self])]
                                          /\ TRUE
                                          /\ pc' = [pc EXCEPT ![self] = "lbl_exe_scc"]
                                          /\ UNCHANGED << stack, curr_pie, 
                                                          pie_map, next_pie >>
                               /\ UNCHANGED << svr_mq, srz, n_committed, 
                                               id_counter, pre_accept_acks_A, 
                                               partitions_, pre_accept_acks, 
                                               partitions, next_txn_, 
                                               next_pie_, txn_id, dep_c, par, 
                                               n_pie, pre_accept_acks_, 
                                               accept_acks, committed, ballot, 
                                               tmp_ack, tmp_acks, tmp_par, 
                                               tmp_partitions, shards, msg_in_, 
                                               msg_out_, keyspace, dep, ya_dep, 
                                               asked_map, tx_meta, scc, 
                                               scc_set, scc_seq, curr_txn, 
                                               curr_pie_, curr_op, txn_set, 
                                               tid_set, opset, next_txn, 
                                               next_tid, anc, tmp_set, alogs, 
                                               vid_set, sub_graph, tx_meta_set, 
                                               msg_in, svr_end >>

ExeSccSet(self) == lbl_exe_scc_set(self) \/ lbl_exe_scc(self)
                      \/ __debug_label_zcxvx(self)
                      \/ __debug_label_kdjf(self)
                      \/ exe_all_deferred_pies(self)

lbl_on_commit(self) == /\ pc[self] = "lbl_on_commit"
                       /\ dep' = [dep EXCEPT ![self][msg_in[self].tid] = [@ EXCEPT !.status = COMMITTING, !.parents = msg_in[self].dep]]
                       /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "InquireUnknown",
                                                                pc        |->  "lbl_choose_to_commit" ] >>
                                                            \o stack[self]]
                       /\ pc' = [pc EXCEPT ![self] = "lbl_inquire_unknown"]
                       /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                       id_counter, pre_accept_acks_A, 
                                       partitions_, pre_accept_acks, 
                                       partitions, curr_pie, next_txn_, 
                                       next_pie_, txn_id, dep_c, par, n_pie, 
                                       pre_accept_acks_, accept_acks, 
                                       committed, ballot, tmp_ack, tmp_acks, 
                                       tmp_par, tmp_partitions, shards, 
                                       msg_in_, msg_out_, keyspace, ya_dep, 
                                       finished_map, asked_map, pie_map, 
                                       tx_meta, scc, scc_set, scc_seq, 
                                       curr_txn, curr_pie_, curr_op, txn_set, 
                                       tid_set, opset, next_txn, next_tid, 
                                       next_pie, anc, tmp_set, alogs, vid_set, 
                                       sub_graph, tx_meta_set, msg_in, msg_out, 
                                       svr_end >>

lbl_choose_to_commit(self) == /\ pc[self] = "lbl_choose_to_commit"
                              /\ txn_set' = [txn_set EXCEPT ![self] = AllAncestorsAtLeastCommittingTidSet(dep[self])]
                              /\ pc' = [pc EXCEPT ![self] = "lbl_ready_to_commit"]
                              /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                              id_counter, stack, 
                                              pre_accept_acks_A, partitions_, 
                                              pre_accept_acks, partitions, 
                                              curr_pie, next_txn_, next_pie_, 
                                              txn_id, dep_c, par, n_pie, 
                                              pre_accept_acks_, accept_acks, 
                                              committed, ballot, tmp_ack, 
                                              tmp_acks, tmp_par, 
                                              tmp_partitions, shards, msg_in_, 
                                              msg_out_, keyspace, dep, ya_dep, 
                                              finished_map, asked_map, pie_map, 
                                              tx_meta, scc, scc_set, scc_seq, 
                                              curr_txn, curr_pie_, curr_op, 
                                              tid_set, opset, next_txn, 
                                              next_tid, next_pie, anc, tmp_set, 
                                              alogs, vid_set, sub_graph, 
                                              tx_meta_set, msg_in, msg_out, 
                                              svr_end >>

lbl_ready_to_commit(self) == /\ pc[self] = "lbl_ready_to_commit"
                             /\ IF txn_set[self] /= {}
                                   THEN /\ \E txn \in txn_set[self]:
                                             /\ tid_set' = [tid_set EXCEPT ![self] = SccTidSet(dep[self], txn)]
                                             /\ txn_set' = [txn_set EXCEPT ![self] = txn_set[self] \ {txn}]
                                             /\ dep' = [dep EXCEPT ![self] = UpdateSubGraphStatus(dep[self], tid_set'[self], DECIDED)]
                                        /\ pc' = [pc EXCEPT ![self] = "lbl_ready_to_commit"]
                                   ELSE /\ pc' = [pc EXCEPT ![self] = "lbl_find_exe"]
                                        /\ UNCHANGED << dep, txn_set, tid_set >>
                             /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                             id_counter, stack, 
                                             pre_accept_acks_A, partitions_, 
                                             pre_accept_acks, partitions, 
                                             curr_pie, next_txn_, next_pie_, 
                                             txn_id, dep_c, par, n_pie, 
                                             pre_accept_acks_, accept_acks, 
                                             committed, ballot, tmp_ack, 
                                             tmp_acks, tmp_par, tmp_partitions, 
                                             shards, msg_in_, msg_out_, 
                                             keyspace, ya_dep, finished_map, 
                                             asked_map, pie_map, tx_meta, scc, 
                                             scc_set, scc_seq, curr_txn, 
                                             curr_pie_, curr_op, opset, 
                                             next_txn, next_tid, next_pie, anc, 
                                             tmp_set, alogs, vid_set, 
                                             sub_graph, tx_meta_set, msg_in, 
                                             msg_out, svr_end >>

lbl_find_exe(self) == /\ pc[self] = "lbl_find_exe"
                      /\ scc_set' = [scc_set EXCEPT ![self] = ToExecuteSccTidSet(dep[self], ProcIdToPartitionId(self), finished_map[self])]
                      /\ IF scc_set'[self] /= {}
                            THEN /\ pc' = [pc EXCEPT ![self] = "lbl_next_exe"]
                            ELSE /\ TRUE
                                 /\ pc' = [pc EXCEPT ![self] = "lbl_on_commit_ret"]
                      /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                      id_counter, stack, pre_accept_acks_A, 
                                      partitions_, pre_accept_acks, partitions, 
                                      curr_pie, next_txn_, next_pie_, txn_id, 
                                      dep_c, par, n_pie, pre_accept_acks_, 
                                      accept_acks, committed, ballot, tmp_ack, 
                                      tmp_acks, tmp_par, tmp_partitions, 
                                      shards, msg_in_, msg_out_, keyspace, dep, 
                                      ya_dep, finished_map, asked_map, pie_map, 
                                      tx_meta, scc, scc_seq, curr_txn, 
                                      curr_pie_, curr_op, txn_set, tid_set, 
                                      opset, next_txn, next_tid, next_pie, anc, 
                                      tmp_set, alogs, vid_set, sub_graph, 
                                      tx_meta_set, msg_in, msg_out, svr_end >>

lbl_next_exe(self) == /\ pc[self] = "lbl_next_exe"
                      /\ IF scc_set[self] /= {}
                            THEN /\ \E next_scc \in scc_set[self]:
                                      /\ scc_set' = [scc_set EXCEPT ![self] = scc_set[self] \ {next_scc}]
                                      /\ scc' = [scc EXCEPT ![self] = next_scc]
                                 /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "ExeSccSet",
                                                                          pc        |->  "lbl_next_exe" ] >>
                                                                      \o stack[self]]
                                 /\ pc' = [pc EXCEPT ![self] = "lbl_exe_scc_set"]
                            ELSE /\ pc' = [pc EXCEPT ![self] = "lbl_find_exe"]
                                 /\ UNCHANGED << stack, scc, scc_set >>
                      /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                      id_counter, pre_accept_acks_A, 
                                      partitions_, pre_accept_acks, partitions, 
                                      curr_pie, next_txn_, next_pie_, txn_id, 
                                      dep_c, par, n_pie, pre_accept_acks_, 
                                      accept_acks, committed, ballot, tmp_ack, 
                                      tmp_acks, tmp_par, tmp_partitions, 
                                      shards, msg_in_, msg_out_, keyspace, dep, 
                                      ya_dep, finished_map, asked_map, pie_map, 
                                      tx_meta, scc_seq, curr_txn, curr_pie_, 
                                      curr_op, txn_set, tid_set, opset, 
                                      next_txn, next_tid, next_pie, anc, 
                                      tmp_set, alogs, vid_set, sub_graph, 
                                      tx_meta_set, msg_in, msg_out, svr_end >>

lbl_on_commit_ret(self) == /\ pc[self] = "lbl_on_commit_ret"
                           /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                           /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                           /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                           id_counter, pre_accept_acks_A, 
                                           partitions_, pre_accept_acks, 
                                           partitions, curr_pie, next_txn_, 
                                           next_pie_, txn_id, dep_c, par, 
                                           n_pie, pre_accept_acks_, 
                                           accept_acks, committed, ballot, 
                                           tmp_ack, tmp_acks, tmp_par, 
                                           tmp_partitions, shards, msg_in_, 
                                           msg_out_, keyspace, dep, ya_dep, 
                                           finished_map, asked_map, pie_map, 
                                           tx_meta, scc, scc_set, scc_seq, 
                                           curr_txn, curr_pie_, curr_op, 
                                           txn_set, tid_set, opset, next_txn, 
                                           next_tid, next_pie, anc, tmp_set, 
                                           alogs, vid_set, sub_graph, 
                                           tx_meta_set, msg_in, msg_out, 
                                           svr_end >>

HandleCommit(self) == lbl_on_commit(self) \/ lbl_choose_to_commit(self)
                         \/ lbl_ready_to_commit(self) \/ lbl_find_exe(self)
                         \/ lbl_next_exe(self) \/ lbl_on_commit_ret(self)

lbl_on_inquire(self) == /\ pc[self] = "lbl_on_inquire"
                        /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                        /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                        /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                        id_counter, pre_accept_acks_A, 
                                        partitions_, pre_accept_acks, 
                                        partitions, curr_pie, next_txn_, 
                                        next_pie_, txn_id, dep_c, par, n_pie, 
                                        pre_accept_acks_, accept_acks, 
                                        committed, ballot, tmp_ack, tmp_acks, 
                                        tmp_par, tmp_partitions, shards, 
                                        msg_in_, msg_out_, keyspace, dep, 
                                        ya_dep, finished_map, asked_map, 
                                        pie_map, tx_meta, scc, scc_set, 
                                        scc_seq, curr_txn, curr_pie_, curr_op, 
                                        txn_set, tid_set, opset, next_txn, 
                                        next_tid, next_pie, anc, tmp_set, 
                                        alogs, vid_set, sub_graph, tx_meta_set, 
                                        msg_in, msg_out, svr_end >>

HandleInquire(self) == lbl_on_inquire(self)

lbl_exe_pie(self) == /\ pc[self] = "lbl_exe_pie"
                     /\ opset' = [opset EXCEPT ![self] = curr_pie[self].opset]
                     /\ Assert((opset'[self] /= {}), 
                               "Failure of assertion at line 803, column 5.")
                     /\ pc' = [pc EXCEPT ![self] = "lbl_pie_add_srz"]
                     /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                     id_counter, stack, pre_accept_acks_A, 
                                     partitions_, pre_accept_acks, partitions, 
                                     curr_pie, next_txn_, next_pie_, txn_id, 
                                     dep_c, par, n_pie, pre_accept_acks_, 
                                     accept_acks, committed, ballot, tmp_ack, 
                                     tmp_acks, tmp_par, tmp_partitions, shards, 
                                     msg_in_, msg_out_, keyspace, dep, ya_dep, 
                                     finished_map, asked_map, pie_map, tx_meta, 
                                     scc, scc_set, scc_seq, curr_txn, 
                                     curr_pie_, curr_op, txn_set, tid_set, 
                                     next_txn, next_tid, next_pie, anc, 
                                     tmp_set, alogs, vid_set, sub_graph, 
                                     tx_meta_set, msg_in, msg_out, svr_end >>

lbl_pie_add_srz(self) == /\ pc[self] = "lbl_pie_add_srz"
                         /\ IF opset[self] /= {}
                               THEN /\ \E next_op \in opset[self]:
                                         /\ curr_op' = [curr_op EXCEPT ![self] = next_op]
                                         /\ opset' = [opset EXCEPT ![self] = opset[self] \ {next_op}]
                                    /\ srz' = SrzAddNode(srz, curr_txn[self].tid)
                                    /\ alogs' = [alogs EXCEPT ![self] = keyspace[self][curr_op'[self].key].alogs]
                                    /\ pc' = [pc EXCEPT ![self] = "lbl_scan_alogs"]
                                    /\ UNCHANGED << stack, curr_pie >>
                               ELSE /\ pc' = [pc EXCEPT ![self] = Head(stack[self]).pc]
                                    /\ curr_pie' = [curr_pie EXCEPT ![self] = Head(stack[self]).curr_pie]
                                    /\ stack' = [stack EXCEPT ![self] = Tail(stack[self])]
                                    /\ UNCHANGED << srz, curr_op, opset, alogs >>
                         /\ UNCHANGED << coo_mq, svr_mq, n_committed, 
                                         id_counter, pre_accept_acks_A, 
                                         partitions_, pre_accept_acks, 
                                         partitions, next_txn_, next_pie_, 
                                         txn_id, dep_c, par, n_pie, 
                                         pre_accept_acks_, accept_acks, 
                                         committed, ballot, tmp_ack, tmp_acks, 
                                         tmp_par, tmp_partitions, shards, 
                                         msg_in_, msg_out_, keyspace, dep, 
                                         ya_dep, finished_map, asked_map, 
                                         pie_map, tx_meta, scc, scc_set, 
                                         scc_seq, curr_txn, curr_pie_, txn_set, 
                                         tid_set, next_txn, next_tid, next_pie, 
                                         anc, tmp_set, vid_set, sub_graph, 
                                         tx_meta_set, msg_in, msg_out, svr_end >>

lbl_scan_alogs(self) == /\ pc[self] = "lbl_scan_alogs"
                        /\ IF alogs[self] /= {}
                              THEN /\ \E next_access \in alogs[self]:
                                        /\ alogs' = [alogs EXCEPT ![self] = alogs[self] \ {next_access}]
                                        /\ IF \/ next_access.type /= curr_op[self].type
                                              \/ next_access.type = "w"
                                              THEN /\ srz' = SrzAddEdge(srz, next_access.tid, curr_txn[self].tid)
                                              ELSE /\ TRUE
                                                   /\ srz' = srz
                                        /\ keyspace' = [keyspace EXCEPT ![self][curr_op[self].key].logs = @ \union {<<curr_txn[self].tid, curr_op[self].type>>}]
                                   /\ pc' = [pc EXCEPT ![self] = "lbl_scan_alogs"]
                              ELSE /\ pc' = [pc EXCEPT ![self] = "lbl_pie_add_srz"]
                                   /\ UNCHANGED << srz, keyspace, alogs >>
                        /\ UNCHANGED << coo_mq, svr_mq, n_committed, 
                                        id_counter, stack, pre_accept_acks_A, 
                                        partitions_, pre_accept_acks, 
                                        partitions, curr_pie, next_txn_, 
                                        next_pie_, txn_id, dep_c, par, n_pie, 
                                        pre_accept_acks_, accept_acks, 
                                        committed, ballot, tmp_ack, tmp_acks, 
                                        tmp_par, tmp_partitions, shards, 
                                        msg_in_, msg_out_, dep, ya_dep, 
                                        finished_map, asked_map, pie_map, 
                                        tx_meta, scc, scc_set, scc_seq, 
                                        curr_txn, curr_pie_, curr_op, txn_set, 
                                        tid_set, opset, next_txn, next_tid, 
                                        next_pie, anc, tmp_set, vid_set, 
                                        sub_graph, tx_meta_set, msg_in, 
                                        msg_out, svr_end >>

ExePie(self) == lbl_exe_pie(self) \/ lbl_pie_add_srz(self)
                   \/ lbl_scan_alogs(self)

lbl_coord_begin(self) == /\ pc[self] = "lbl_coord_begin"
                         /\ dep_c' = [dep_c EXCEPT ![self] = {}]
                         /\ shards' = [shards EXCEPT ![self] = {}]
                         /\ id_counter' = id_counter + 1
                         /\ txn_id' = [txn_id EXCEPT ![self] = self]
                         /\ Assert((txn_id'[self] > 0), 
                                   "Failure of assertion at line 855, column 5.")
                         /\ committed' = [committed EXCEPT ![self] = FALSE]
                         /\ next_txn_' = [next_txn_ EXCEPT ![self] = TXN[self]]
                         /\ Assert((IsTxnRequest(next_txn_'[self])), 
                                   "Failure of assertion at line 859, column 5.")
                         /\ n_pie' = [n_pie EXCEPT ![self] = Len(next_txn_'[self])]
                         /\ Assert((n_pie'[self] > 0), 
                                   "Failure of assertion at line 861, column 5.")
                         /\ pc' = [pc EXCEPT ![self] = "lbl_get_shards"]
                         /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                         stack, pre_accept_acks_A, partitions_, 
                                         pre_accept_acks, partitions, curr_pie, 
                                         next_pie_, par, pre_accept_acks_, 
                                         accept_acks, ballot, tmp_ack, 
                                         tmp_acks, tmp_par, tmp_partitions, 
                                         msg_in_, msg_out_, keyspace, dep, 
                                         ya_dep, finished_map, asked_map, 
                                         pie_map, tx_meta, scc, scc_set, 
                                         scc_seq, curr_txn, curr_pie_, curr_op, 
                                         txn_set, tid_set, opset, next_txn, 
                                         next_tid, next_pie, anc, tmp_set, 
                                         alogs, vid_set, sub_graph, 
                                         tx_meta_set, msg_in, msg_out, svr_end >>

lbl_get_shards(self) == /\ pc[self] = "lbl_get_shards"
                        /\ shards' = [shards EXCEPT ![self] = GetTxShards(next_txn_[self])]
                        /\ Assert((shards'[self] /= {}), 
                                  "Failure of assertion at line 864, column 5.")
                        /\ pc' = [pc EXCEPT ![self] = "lbl_pre_accept"]
                        /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                        id_counter, stack, pre_accept_acks_A, 
                                        partitions_, pre_accept_acks, 
                                        partitions, curr_pie, next_txn_, 
                                        next_pie_, txn_id, dep_c, par, n_pie, 
                                        pre_accept_acks_, accept_acks, 
                                        committed, ballot, tmp_ack, tmp_acks, 
                                        tmp_par, tmp_partitions, msg_in_, 
                                        msg_out_, keyspace, dep, ya_dep, 
                                        finished_map, asked_map, pie_map, 
                                        tx_meta, scc, scc_set, scc_seq, 
                                        curr_txn, curr_pie_, curr_op, txn_set, 
                                        tid_set, opset, next_txn, next_tid, 
                                        next_pie, anc, tmp_set, alogs, vid_set, 
                                        sub_graph, tx_meta_set, msg_in, 
                                        msg_out, svr_end >>

lbl_pre_accept(self) == /\ pc[self] = "lbl_pre_accept"
                        /\ IF Len(next_txn_[self]) > 0
                              THEN /\ next_pie_' = [next_pie_ EXCEPT ![self] = Head(next_txn_[self])]
                                   /\ next_txn_' = [next_txn_ EXCEPT ![self] = Tail(next_txn_[self])]
                                   /\ par' = [par EXCEPT ![self] = Shard(next_pie_'[self])]
                                   /\ msg_out_' = [msg_out_ EXCEPT ![self] = [
                                                                                type |-> "pre_accept",
                                                                                src |-> self,
                                                                                tid |-> txn_id[self],
                                                                                pie |-> next_pie_'[self],
                                                                                meta |-> GenTxMeta(txn_id[self], shards[self])
                                                                             ]]
                                   /\ Assert((IsMsgValid(msg_out_'[self])), 
                                             "Failure of assertion at line 878, column 7.")
                                   /\ svr_mq' = BroadcastMsg(svr_mq, par'[self], msg_out_'[self])
                                   /\ pc' = [pc EXCEPT ![self] = "lbl_pre_accept"]
                                   /\ UNCHANGED pre_accept_acks_
                              ELSE /\ pre_accept_acks_' = [pre_accept_acks_ EXCEPT ![self] = InitAcks(shards[self])]
                                   /\ pc' = [pc EXCEPT ![self] = "lbl_coord_pre_accept_ack"]
                                   /\ UNCHANGED << svr_mq, next_txn_, 
                                                   next_pie_, par, msg_out_ >>
                        /\ UNCHANGED << coo_mq, srz, n_committed, id_counter, 
                                        stack, pre_accept_acks_A, partitions_, 
                                        pre_accept_acks, partitions, curr_pie, 
                                        txn_id, dep_c, n_pie, accept_acks, 
                                        committed, ballot, tmp_ack, tmp_acks, 
                                        tmp_par, tmp_partitions, shards, 
                                        msg_in_, keyspace, dep, ya_dep, 
                                        finished_map, asked_map, pie_map, 
                                        tx_meta, scc, scc_set, scc_seq, 
                                        curr_txn, curr_pie_, curr_op, txn_set, 
                                        tid_set, opset, next_txn, next_tid, 
                                        next_pie, anc, tmp_set, alogs, vid_set, 
                                        sub_graph, tx_meta_set, msg_in, 
                                        msg_out, svr_end >>

lbl_coord_pre_accept_ack(self) == /\ pc[self] = "lbl_coord_pre_accept_ack"
                                  /\ Len(coo_mq[self]) > 0
                                  /\ msg_in_' = [msg_in_ EXCEPT ![self] = Head(coo_mq[self])]
                                  /\ coo_mq' = [coo_mq EXCEPT ![self] = Tail(coo_mq[self])]
                                  /\ Assert((IsMsgValid(msg_in_'[self])), 
                                            "Failure of assertion at line 889, column 7.")
                                  /\ IF msg_in_'[self].type = "ack_pre_accept" /\ msg_in_'[self].tid = txn_id[self]
                                        THEN /\ par' = [par EXCEPT ![self] = ProcIdToPartitionId(msg_in_'[self].src)]
                                             /\ pre_accept_acks_' = [pre_accept_acks_ EXCEPT ![self][par'[self]] = pre_accept_acks_[self][par'[self]] \union {msg_in_'[self]}]
                                             /\ IF FastPathPossible(pre_accept_acks_'[self], shards[self])
                                                   THEN /\ IF FastPathReady(pre_accept_acks_'[self], shards[self])
                                                              THEN /\ /\ partitions_' = [partitions_ EXCEPT ![self] = shards[self]]
                                                                      /\ pre_accept_acks_A' = [pre_accept_acks_A EXCEPT ![self] = pre_accept_acks_'[self]]
                                                                      /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "AggregateGraphForFastPath",
                                                                                                               pc        |->  "lbl_coord_goto_commit",
                                                                                                               pre_accept_acks_A |->  pre_accept_acks_A[self],
                                                                                                               partitions_ |->  partitions_[self] ] >>
                                                                                                           \o stack[self]]
                                                                   /\ pc' = [pc EXCEPT ![self] = "xxxxdddx"]
                                                              ELSE /\ pc' = [pc EXCEPT ![self] = "lbl_dbg_zzzzz"]
                                                                   /\ UNCHANGED << stack, 
                                                                                   pre_accept_acks_A, 
                                                                                   partitions_ >>
                                                   ELSE /\ IF M = 1
                                                              THEN /\ Assert((FALSE), 
                                                                             "Failure of assertion at line 909, column 13.")
                                                              ELSE /\ TRUE
                                                        /\ pc' = [pc EXCEPT ![self] = "lbl_coord_pre_accept_ack"]
                                                        /\ UNCHANGED << stack, 
                                                                        pre_accept_acks_A, 
                                                                        partitions_ >>
                                        ELSE /\ Assert((FALSE), 
                                                       "Failure of assertion at line 921, column 9.")
                                             /\ pc' = [pc EXCEPT ![self] = "lbl_coord_pre_accept_ack"]
                                             /\ UNCHANGED << stack, 
                                                             pre_accept_acks_A, 
                                                             partitions_, par, 
                                                             pre_accept_acks_ >>
                                  /\ UNCHANGED << svr_mq, srz, n_committed, 
                                                  id_counter, pre_accept_acks, 
                                                  partitions, curr_pie, 
                                                  next_txn_, next_pie_, txn_id, 
                                                  dep_c, n_pie, accept_acks, 
                                                  committed, ballot, tmp_ack, 
                                                  tmp_acks, tmp_par, 
                                                  tmp_partitions, shards, 
                                                  msg_out_, keyspace, dep, 
                                                  ya_dep, finished_map, 
                                                  asked_map, pie_map, tx_meta, 
                                                  scc, scc_set, scc_seq, 
                                                  curr_txn, curr_pie_, curr_op, 
                                                  txn_set, tid_set, opset, 
                                                  next_txn, next_tid, next_pie, 
                                                  anc, tmp_set, alogs, vid_set, 
                                                  sub_graph, tx_meta_set, 
                                                  msg_in, msg_out, svr_end >>

lbl_coord_goto_commit(self) == /\ pc[self] = "lbl_coord_goto_commit"
                               /\ pc' = [pc EXCEPT ![self] = "commit_phase"]
                               /\ UNCHANGED << coo_mq, svr_mq, srz, 
                                               n_committed, id_counter, stack, 
                                               pre_accept_acks_A, partitions_, 
                                               pre_accept_acks, partitions, 
                                               curr_pie, next_txn_, next_pie_, 
                                               txn_id, dep_c, par, n_pie, 
                                               pre_accept_acks_, accept_acks, 
                                               committed, ballot, tmp_ack, 
                                               tmp_acks, tmp_par, 
                                               tmp_partitions, shards, msg_in_, 
                                               msg_out_, keyspace, dep, ya_dep, 
                                               finished_map, asked_map, 
                                               pie_map, tx_meta, scc, scc_set, 
                                               scc_seq, curr_txn, curr_pie_, 
                                               curr_op, txn_set, tid_set, 
                                               opset, next_txn, next_tid, 
                                               next_pie, anc, tmp_set, alogs, 
                                               vid_set, sub_graph, tx_meta_set, 
                                               msg_in, msg_out, svr_end >>

lbl_dbg_zzzzz(self) == /\ pc[self] = "lbl_dbg_zzzzz"
                       /\ IF N=1 /\ X=1
                             THEN /\ Assert((FALSE), 
                                            "Failure of assertion at line 904, column 15.")
                             ELSE /\ TRUE
                       /\ pc' = [pc EXCEPT ![self] = "lbl_coord_pre_accept_ack"]
                       /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                       id_counter, stack, pre_accept_acks_A, 
                                       partitions_, pre_accept_acks, 
                                       partitions, curr_pie, next_txn_, 
                                       next_pie_, txn_id, dep_c, par, n_pie, 
                                       pre_accept_acks_, accept_acks, 
                                       committed, ballot, tmp_ack, tmp_acks, 
                                       tmp_par, tmp_partitions, shards, 
                                       msg_in_, msg_out_, keyspace, dep, 
                                       ya_dep, finished_map, asked_map, 
                                       pie_map, tx_meta, scc, scc_set, scc_seq, 
                                       curr_txn, curr_pie_, curr_op, txn_set, 
                                       tid_set, opset, next_txn, next_tid, 
                                       next_pie, anc, tmp_set, alogs, vid_set, 
                                       sub_graph, tx_meta_set, msg_in, msg_out, 
                                       svr_end >>

accept_phase(self) == /\ pc[self] = "accept_phase"
                      /\ ballot' = [ballot EXCEPT ![self] = DEFAULT_ACCEPT_BALLOT]
                      /\ msg_out_' = [msg_out_ EXCEPT ![self] =            [
                                                                  type |-> "accept",
                                                                  ballot |-> DEFAULT_ACCEPT_BALLOT,
                                                                  src |-> self,
                                                                  tid |-> txn_id[self],
                                                                  dep |-> dep_c[self]
                                                                ]]
                      /\ pc' = [pc EXCEPT ![self] = "lbl_dbg_jjjjj"]
                      /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                      id_counter, stack, pre_accept_acks_A, 
                                      partitions_, pre_accept_acks, partitions, 
                                      curr_pie, next_txn_, next_pie_, txn_id, 
                                      dep_c, par, n_pie, pre_accept_acks_, 
                                      accept_acks, committed, tmp_ack, 
                                      tmp_acks, tmp_par, tmp_partitions, 
                                      shards, msg_in_, keyspace, dep, ya_dep, 
                                      finished_map, asked_map, pie_map, 
                                      tx_meta, scc, scc_set, scc_seq, curr_txn, 
                                      curr_pie_, curr_op, txn_set, tid_set, 
                                      opset, next_txn, next_tid, next_pie, anc, 
                                      tmp_set, alogs, vid_set, sub_graph, 
                                      tx_meta_set, msg_in, msg_out, svr_end >>

lbl_dbg_jjjjj(self) == /\ pc[self] = "lbl_dbg_jjjjj"
                       /\ Assert((msg_out_[self].ballot \geq 0), 
                                 "Failure of assertion at line 935, column 20.")
                       /\ pc' = [pc EXCEPT ![self] = "broadcast_accept"]
                       /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                       id_counter, stack, pre_accept_acks_A, 
                                       partitions_, pre_accept_acks, 
                                       partitions, curr_pie, next_txn_, 
                                       next_pie_, txn_id, dep_c, par, n_pie, 
                                       pre_accept_acks_, accept_acks, 
                                       committed, ballot, tmp_ack, tmp_acks, 
                                       tmp_par, tmp_partitions, shards, 
                                       msg_in_, msg_out_, keyspace, dep, 
                                       ya_dep, finished_map, asked_map, 
                                       pie_map, tx_meta, scc, scc_set, scc_seq, 
                                       curr_txn, curr_pie_, curr_op, txn_set, 
                                       tid_set, opset, next_txn, next_tid, 
                                       next_pie, anc, tmp_set, alogs, vid_set, 
                                       sub_graph, tx_meta_set, msg_in, msg_out, 
                                       svr_end >>

broadcast_accept(self) == /\ pc[self] = "broadcast_accept"
                          /\ svr_mq' = BroadcastMsgToMultiplePartitions(svr_mq, shards[self], msg_out_[self])
                          /\ accept_acks' = [accept_acks EXCEPT ![self] = InitAcks(shards[self])]
                          /\ pc' = [pc EXCEPT ![self] = "process_accept_ack"]
                          /\ UNCHANGED << coo_mq, srz, n_committed, id_counter, 
                                          stack, pre_accept_acks_A, 
                                          partitions_, pre_accept_acks, 
                                          partitions, curr_pie, next_txn_, 
                                          next_pie_, txn_id, dep_c, par, n_pie, 
                                          pre_accept_acks_, committed, ballot, 
                                          tmp_ack, tmp_acks, tmp_par, 
                                          tmp_partitions, shards, msg_in_, 
                                          msg_out_, keyspace, dep, ya_dep, 
                                          finished_map, asked_map, pie_map, 
                                          tx_meta, scc, scc_set, scc_seq, 
                                          curr_txn, curr_pie_, curr_op, 
                                          txn_set, tid_set, opset, next_txn, 
                                          next_tid, next_pie, anc, tmp_set, 
                                          alogs, vid_set, sub_graph, 
                                          tx_meta_set, msg_in, msg_out, 
                                          svr_end >>

process_accept_ack(self) == /\ pc[self] = "process_accept_ack"
                            /\ Len(coo_mq[self]) > 0
                            /\ msg_in_' = [msg_in_ EXCEPT ![self] = Head(coo_mq[self])]
                            /\ coo_mq' = [coo_mq EXCEPT ![self] = Tail(coo_mq[self])]
                            /\ IF /\ msg_in_'[self].type = "ack_accept"
                                  /\ msg_in_'[self].tid = txn_id[self]
                                  /\ msg_in_'[self].ballot = ballot[self]
                                  THEN /\ par' = [par EXCEPT ![self] = ProcIdToPartitionId(msg_in_'[self].src)]
                                       /\ accept_acks' = [accept_acks EXCEPT ![self][par'[self]] = @ \union {msg_in_'[self]}]
                                       /\ IF CommitReady(accept_acks'[self], shards[self])
                                             THEN /\ pc' = [pc EXCEPT ![self] = "commit_phase"]
                                             ELSE /\ pc' = [pc EXCEPT ![self] = "process_accept_ack"]
                                  ELSE /\ IF msg_in_'[self].type /= "ack_pre_accept"
                                             THEN /\ pc' = [pc EXCEPT ![self] = "__debug_lable_gkdjfk"]
                                             ELSE /\ pc' = [pc EXCEPT ![self] = "process_accept_ack"]
                                       /\ UNCHANGED << par, accept_acks >>
                            /\ UNCHANGED << svr_mq, srz, n_committed, 
                                            id_counter, stack, 
                                            pre_accept_acks_A, partitions_, 
                                            pre_accept_acks, partitions, 
                                            curr_pie, next_txn_, next_pie_, 
                                            txn_id, dep_c, n_pie, 
                                            pre_accept_acks_, committed, 
                                            ballot, tmp_ack, tmp_acks, tmp_par, 
                                            tmp_partitions, shards, msg_out_, 
                                            keyspace, dep, ya_dep, 
                                            finished_map, asked_map, pie_map, 
                                            tx_meta, scc, scc_set, scc_seq, 
                                            curr_txn, curr_pie_, curr_op, 
                                            txn_set, tid_set, opset, next_txn, 
                                            next_tid, next_pie, anc, tmp_set, 
                                            alogs, vid_set, sub_graph, 
                                            tx_meta_set, msg_in, msg_out, 
                                            svr_end >>

__debug_lable_gkdjfk(self) == /\ pc[self] = "__debug_lable_gkdjfk"
                              /\ Assert((FALSE), 
                                        "Failure of assertion at line 954, column 33.")
                              /\ pc' = [pc EXCEPT ![self] = "process_accept_ack"]
                              /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                              id_counter, stack, 
                                              pre_accept_acks_A, partitions_, 
                                              pre_accept_acks, partitions, 
                                              curr_pie, next_txn_, next_pie_, 
                                              txn_id, dep_c, par, n_pie, 
                                              pre_accept_acks_, accept_acks, 
                                              committed, ballot, tmp_ack, 
                                              tmp_acks, tmp_par, 
                                              tmp_partitions, shards, msg_in_, 
                                              msg_out_, keyspace, dep, ya_dep, 
                                              finished_map, asked_map, pie_map, 
                                              tx_meta, scc, scc_set, scc_seq, 
                                              curr_txn, curr_pie_, curr_op, 
                                              txn_set, tid_set, opset, 
                                              next_txn, next_tid, next_pie, 
                                              anc, tmp_set, alogs, vid_set, 
                                              sub_graph, tx_meta_set, msg_in, 
                                              msg_out, svr_end >>

commit_phase(self) == /\ pc[self] = "commit_phase"
                      /\ msg_out_' = [msg_out_ EXCEPT ![self] =            [
                                                                  type |-> "commit",
                                                                  src |-> self,
                                                                  tid |-> txn_id[self],
                                                                  dep |-> dep_c[self]
                                                                ]]
                      /\ Assert((IsMsgValid(msg_out_'[self])), 
                                "Failure of assertion at line 968, column 5.")
                      /\ svr_mq' = BroadcastMsgToMultiplePartitions(svr_mq, shards[self], msg_out_'[self])
                      /\ pc' = [pc EXCEPT ![self] = "lbl_wait_commit_ack"]
                      /\ UNCHANGED << coo_mq, srz, n_committed, id_counter, 
                                      stack, pre_accept_acks_A, partitions_, 
                                      pre_accept_acks, partitions, curr_pie, 
                                      next_txn_, next_pie_, txn_id, dep_c, par, 
                                      n_pie, pre_accept_acks_, accept_acks, 
                                      committed, ballot, tmp_ack, tmp_acks, 
                                      tmp_par, tmp_partitions, shards, msg_in_, 
                                      keyspace, dep, ya_dep, finished_map, 
                                      asked_map, pie_map, tx_meta, scc, 
                                      scc_set, scc_seq, curr_txn, curr_pie_, 
                                      curr_op, txn_set, tid_set, opset, 
                                      next_txn, next_tid, next_pie, anc, 
                                      tmp_set, alogs, vid_set, sub_graph, 
                                      tx_meta_set, msg_in, msg_out, svr_end >>

lbl_wait_commit_ack(self) == /\ pc[self] = "lbl_wait_commit_ack"
                             /\ (Len(coo_mq[self]) > 0)
                             /\ msg_in_' = [msg_in_ EXCEPT ![self] = Head(coo_mq[self])]
                             /\ coo_mq' = [coo_mq EXCEPT ![self] = Tail(coo_mq[self])]
                             /\ IF msg_in_'[self].type = "ack_commit" /\ msg_in_'[self].tid = txn_id[self]
                                   THEN /\ committed' = [committed EXCEPT ![self] = TRUE]
                                        /\ n_committed' = n_committed + 1
                                        /\ pc' = [pc EXCEPT ![self] = "end_coord"]
                                   ELSE /\ pc' = [pc EXCEPT ![self] = "lbl_dbg_kkkkk"]
                                        /\ UNCHANGED << n_committed, committed >>
                             /\ UNCHANGED << svr_mq, srz, id_counter, stack, 
                                             pre_accept_acks_A, partitions_, 
                                             pre_accept_acks, partitions, 
                                             curr_pie, next_txn_, next_pie_, 
                                             txn_id, dep_c, par, n_pie, 
                                             pre_accept_acks_, accept_acks, 
                                             ballot, tmp_ack, tmp_acks, 
                                             tmp_par, tmp_partitions, shards, 
                                             msg_out_, keyspace, dep, ya_dep, 
                                             finished_map, asked_map, pie_map, 
                                             tx_meta, scc, scc_set, scc_seq, 
                                             curr_txn, curr_pie_, curr_op, 
                                             txn_set, tid_set, opset, next_txn, 
                                             next_tid, next_pie, anc, tmp_set, 
                                             alogs, vid_set, sub_graph, 
                                             tx_meta_set, msg_in, msg_out, 
                                             svr_end >>

lbl_dbg_kkkkk(self) == /\ pc[self] = "lbl_dbg_kkkkk"
                       /\ Assert((msg_in_[self].type /= "ack_commit"), 
                                 "Failure of assertion at line 985, column 9.")
                       /\ pc' = [pc EXCEPT ![self] = "lbl_wait_commit_ack"]
                       /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                       id_counter, stack, pre_accept_acks_A, 
                                       partitions_, pre_accept_acks, 
                                       partitions, curr_pie, next_txn_, 
                                       next_pie_, txn_id, dep_c, par, n_pie, 
                                       pre_accept_acks_, accept_acks, 
                                       committed, ballot, tmp_ack, tmp_acks, 
                                       tmp_par, tmp_partitions, shards, 
                                       msg_in_, msg_out_, keyspace, dep, 
                                       ya_dep, finished_map, asked_map, 
                                       pie_map, tx_meta, scc, scc_set, scc_seq, 
                                       curr_txn, curr_pie_, curr_op, txn_set, 
                                       tid_set, opset, next_txn, next_tid, 
                                       next_pie, anc, tmp_set, alogs, vid_set, 
                                       sub_graph, tx_meta_set, msg_in, msg_out, 
                                       svr_end >>

end_coord(self) == /\ pc[self] = "end_coord"
                   /\ IF n_committed = M
                         THEN /\ msg_out_' = [msg_out_ EXCEPT ![self] = [type|->"end", src|->self, tid |-> 0]]
                              /\ svr_mq' = BroadcastMsgToMultiplePartitions(svr_mq, 1..N, msg_out_'[self])
                         ELSE /\ TRUE
                              /\ UNCHANGED << svr_mq, msg_out_ >>
                   /\ pc' = [pc EXCEPT ![self] = "Done"]
                   /\ UNCHANGED << coo_mq, srz, n_committed, id_counter, stack, 
                                   pre_accept_acks_A, partitions_, 
                                   pre_accept_acks, partitions, curr_pie, 
                                   next_txn_, next_pie_, txn_id, dep_c, par, 
                                   n_pie, pre_accept_acks_, accept_acks, 
                                   committed, ballot, tmp_ack, tmp_acks, 
                                   tmp_par, tmp_partitions, shards, msg_in_, 
                                   keyspace, dep, ya_dep, finished_map, 
                                   asked_map, pie_map, tx_meta, scc, scc_set, 
                                   scc_seq, curr_txn, curr_pie_, curr_op, 
                                   txn_set, tid_set, opset, next_txn, next_tid, 
                                   next_pie, anc, tmp_set, alogs, vid_set, 
                                   sub_graph, tx_meta_set, msg_in, msg_out, 
                                   svr_end >>

Coo(self) == lbl_coord_begin(self) \/ lbl_get_shards(self)
                \/ lbl_pre_accept(self) \/ lbl_coord_pre_accept_ack(self)
                \/ lbl_coord_goto_commit(self) \/ lbl_dbg_zzzzz(self)
                \/ accept_phase(self) \/ lbl_dbg_jjjjj(self)
                \/ broadcast_accept(self) \/ process_accept_ack(self)
                \/ __debug_lable_gkdjfk(self) \/ commit_phase(self)
                \/ lbl_wait_commit_ack(self) \/ lbl_dbg_kkkkk(self)
                \/ end_coord(self)

lbl_svr_loop(self) == /\ pc[self] = "lbl_svr_loop"
                      /\ Len(svr_mq[self]) > 0
                      /\ msg_in' = [msg_in EXCEPT ![self] = Head(svr_mq[self])]
                      /\ svr_mq' = [svr_mq EXCEPT ![self] = Tail(svr_mq[self])]
                      /\ ya_dep' = [ya_dep EXCEPT ![self] = EmptyNewDep]
                      /\ IF msg_in'[self].tid > 0
                            THEN /\ curr_txn' = [curr_txn EXCEPT ![self] = FindOrCreateTx(dep[self], msg_in'[self].tid, msg_in'[self].meta)]
                            ELSE /\ Assert((msg_in'[self].type = "end"), 
                                           "Failure of assertion at line 1039, column 7.")
                                 /\ UNCHANGED curr_txn
                      /\ IF /\ msg_in'[self].type = "pre_accept"
                            /\ curr_txn'[self].max_prepared_ballot = 0
                            /\ curr_txn'[self].status = UNKNOWN
                            THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "HandlePreAccept",
                                                                          pc        |->  "lbl_svr_loop_end" ] >>
                                                                      \o stack[self]]
                                 /\ pc' = [pc EXCEPT ![self] = "lbl_on_pre_accept"]
                                 /\ UNCHANGED svr_end
                            ELSE /\ IF \/ msg_in'[self].type = "commit"
                                       \/ msg_in'[self].type = "ack_inquire"
                                       THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "HandleCommit",
                                                                                     pc        |->  "lbl_svr_loop_end" ] >>
                                                                                 \o stack[self]]
                                            /\ pc' = [pc EXCEPT ![self] = "lbl_on_commit"]
                                            /\ UNCHANGED svr_end
                                       ELSE /\ IF msg_in'[self].type = "inquire"
                                                  THEN /\ TRUE
                                                       /\ pc' = [pc EXCEPT ![self] = "lbl_svr_loop_end"]
                                                       /\ UNCHANGED << stack, 
                                                                       svr_end >>
                                                  ELSE /\ IF msg_in'[self].type = "prepare" /\ curr_txn'[self].max_prepared_ballot < msg_in'[self].ballot
                                                             THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "HandlePrepare",
                                                                                                           pc        |->  "lbl_svr_loop_end" ] >>
                                                                                                       \o stack[self]]
                                                                  /\ pc' = [pc EXCEPT ![self] = "lbl_on_prepare"]
                                                                  /\ UNCHANGED svr_end
                                                             ELSE /\ IF msg_in'[self].type = "accept" /\ curr_txn'[self].max_prepared_ballot \leq msg_in'[self].ballot
                                                                        THEN /\ stack' = [stack EXCEPT ![self] = << [ procedure |->  "HandleAccept",
                                                                                                                      pc        |->  "lbl_svr_loop_end" ] >>
                                                                                                                  \o stack[self]]
                                                                             /\ pc' = [pc EXCEPT ![self] = "lbl_on_accept_1"]
                                                                             /\ UNCHANGED svr_end
                                                                        ELSE /\ IF msg_in'[self].type = "end"
                                                                                   THEN /\ svr_end' = [svr_end EXCEPT ![self] = TRUE]
                                                                                   ELSE /\ Assert((FALSE), 
                                                                                                  "Failure of assertion at line 1067, column 7.")
                                                                                        /\ UNCHANGED svr_end
                                                                             /\ pc' = [pc EXCEPT ![self] = "lbl_svr_loop_end"]
                                                                             /\ stack' = stack
                      /\ UNCHANGED << coo_mq, srz, n_committed, id_counter, 
                                      pre_accept_acks_A, partitions_, 
                                      pre_accept_acks, partitions, curr_pie, 
                                      next_txn_, next_pie_, txn_id, dep_c, par, 
                                      n_pie, pre_accept_acks_, accept_acks, 
                                      committed, ballot, tmp_ack, tmp_acks, 
                                      tmp_par, tmp_partitions, shards, msg_in_, 
                                      msg_out_, keyspace, dep, finished_map, 
                                      asked_map, pie_map, tx_meta, scc, 
                                      scc_set, scc_seq, curr_pie_, curr_op, 
                                      txn_set, tid_set, opset, next_txn, 
                                      next_tid, next_pie, anc, tmp_set, alogs, 
                                      vid_set, sub_graph, tx_meta_set, msg_out >>

lbl_svr_loop_end(self) == /\ pc[self] = "lbl_svr_loop_end"
                          /\ IF ~svr_end[self]
                                THEN /\ pc' = [pc EXCEPT ![self] = "lbl_svr_loop"]
                                ELSE /\ pc' = [pc EXCEPT ![self] = "Done"]
                          /\ UNCHANGED << coo_mq, svr_mq, srz, n_committed, 
                                          id_counter, stack, pre_accept_acks_A, 
                                          partitions_, pre_accept_acks, 
                                          partitions, curr_pie, next_txn_, 
                                          next_pie_, txn_id, dep_c, par, n_pie, 
                                          pre_accept_acks_, accept_acks, 
                                          committed, ballot, tmp_ack, tmp_acks, 
                                          tmp_par, tmp_partitions, shards, 
                                          msg_in_, msg_out_, keyspace, dep, 
                                          ya_dep, finished_map, asked_map, 
                                          pie_map, tx_meta, scc, scc_set, 
                                          scc_seq, curr_txn, curr_pie_, 
                                          curr_op, txn_set, tid_set, opset, 
                                          next_txn, next_tid, next_pie, anc, 
                                          tmp_set, alogs, vid_set, sub_graph, 
                                          tx_meta_set, msg_in, msg_out, 
                                          svr_end >>

Svr(self) == lbl_svr_loop(self) \/ lbl_svr_loop_end(self)

Next == (\E self \in ProcSet:  \/ UnitTest(self)
                               \/ AggregateGraphForFastPath(self)
                               \/ AggregateGraphForAccept(self)
                               \/ HandlePreAccept(self)
                               \/ HandlePrepare(self) \/ HandleAccept(self)
                               \/ InquireUnknown(self) \/ ExeSccSet(self)
                               \/ HandleCommit(self) \/ HandleInquire(self)
                               \/ ExePie(self))
           \/ (\E self \in 1 .. M: Coo(self))
           \/ (\E self \in M+1 .. M+N*X: Svr(self))
           \/ (* Disjunct to prevent deadlock on termination *)
              ((\A self \in ProcSet: pc[self] = "Done") /\ UNCHANGED vars)

Spec == /\ Init /\ [][Next]_vars
        /\ WF_vars(Next)

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION

-----------------------------------------------------------------------------

Serializability == <>SrzAcyclic(srz)

Progress == n_committed = M

THEOREM Spec => Serializability

=============================================================================
\* Modification History
\* Last modified Sat Dec 31 04:04:57 EST 2016 by shuai
\* Last modified Thu Dec 25 23:34:46 CST 2014 by Shuai
\* Created Mon Dec 15 15:44:26 CST 2014 by Shuai
