#include "../procedure.h"
#include "../rcc/tx.h"
#include "../rcc/graph_marshaler.h"
#include "commo.h"
#include "marshallable.h"

namespace janus {

void JanusCommo::SendDispatch(vector<TxPieceData>& cmd,
                              const function<void(int res,
                                                  TxnOutput& cmd,
                                                  RccGraph& graph)>& callback) {
  rrr::FutureAttr fuattr;
  auto tid = cmd[0].root_id_;
  auto par_id = cmd[0].partition_id_;
  std::function<void(Future*)> cb =
      [callback, tid, par_id](Future* fu) {
        int res;
        TxnOutput output;
        MarshallDeputy md;
        fu->get_reply() >> res >> output >> md;
        if (md.kind_ == MarshallDeputy::EMPTY_GRAPH) {
          RccGraph rgraph;
          auto v = rgraph.CreateV(tid);
          RccTx& info = *v;
//          info.partition_.insert(par_id);
          verify(rgraph.vertex_index().size() > 0);
          callback(res, output, rgraph);
        } else if (md.kind_ == MarshallDeputy::RCC_GRAPH) {
          RccGraph& graph = dynamic_cast<RccGraph&>(*md.sp_data_);
          callback(res, output, graph);
        } else {
          verify(0);
        }
      };
  fuattr.callback = cb;
  auto proxy = NearestProxyForPartition(cmd[0].PartitionId()).second;
  Log_debug("dispatch to %ld", cmd[0].PartitionId());
//  verify(cmd.type_ > 0);
//  verify(cmd.root_type_ > 0);
  Future::safe_release(proxy->async_JanusDispatch(cmd, fuattr));
}

void JanusCommo::SendHandoutRo(SimpleCommand& cmd,
                               const function<void(int res,
                                                   SimpleCommand& cmd,
                                                   map<int,
                                                       mdb::version_t>& vers)>&) {
  verify(0);
}

void JanusCommo::SendInquire(parid_t pid,
                             epoch_t epoch,
                             txnid_t tid,
                             const function<void(RccGraph& graph)>& callback) {
  FutureAttr fuattr;
  function<void(Future*)> cb = [callback](Future* fu) {
    MarshallDeputy md;
    fu->get_reply() >> md;
    verify(0);
//    auto graph = dynamic_cast<RccGraph&>(*md.sp_data_);
//    callback(graph);
  };
  fuattr.callback = cb;
  // TODO fix.
  auto proxy = NearestProxyForPartition(pid).second;
  Future::safe_release(proxy->async_JanusInquire(epoch, tid, fuattr));
}


void JanusCommo::BroadcastPreAccept(
    parid_t par_id,
    txnid_t txn_id,
    ballot_t ballot,
    vector<TxPieceData>& cmds,
    shared_ptr<RccGraph> sp_graph,
    const function<void(int, shared_ptr<RccGraph>)>& callback) {
  verify(rpc_par_proxies_.find(par_id) != rpc_par_proxies_.end());

  bool skip_graph = IsGraphOrphan(*sp_graph, txn_id);
  verify(0);
  for (auto& p : rpc_par_proxies_[par_id]) {
    auto proxy = (p.second);
    verify(proxy != nullptr);
    FutureAttr fuattr;
    fuattr.callback = [callback](Future* fu) {
      int32_t res;
      MarshallDeputy md;
      fu->get_reply() >> res >> md;
      auto sp = dynamic_pointer_cast<RccGraph>(md.sp_data_);
      verify(sp);
      callback(res, sp);
    };
    verify(txn_id > 0);
    Future* f = nullptr;
    if (skip_graph) {
      f = proxy->async_JanusPreAcceptWoGraph(txn_id, RANK_UNDEFINED, cmds, fuattr);
    } else {
      MarshallDeputy md(sp_graph);
      f = proxy->async_JanusPreAccept(txn_id, RANK_UNDEFINED, cmds, md, fuattr);
    }
    Future::safe_release(f);
  }
}

void JanusCommo::BroadcastAccept(parid_t par_id,
                                 txnid_t cmd_id,
                                 ballot_t ballot,
                                 shared_ptr<RccGraph> graph,
                                 const function<void(int)>& callback) {
  verify(0);
  verify(rpc_par_proxies_.find(par_id) != rpc_par_proxies_.end());
  for (auto& p : rpc_par_proxies_[par_id]) {
    auto proxy = (p.second);
    verify(proxy != nullptr);
    FutureAttr fuattr;
    fuattr.callback = [callback](Future* fu) {
      int32_t res;
      fu->get_reply() >> res;
      callback(res);
    };
    verify(cmd_id > 0);
    MarshallDeputy md(graph);
    rank_t rank = RANK_D;
    verify(0);
    Future::safe_release(proxy->async_JanusAccept(cmd_id,
                                                  rank,
                                                  ballot,
                                                  md,
                                                  fuattr));
  }
}

void JanusCommo::BroadcastCommit(
    parid_t par_id,
    txnid_t cmd_id,
    rank_t rank,
    bool need_validation,
    shared_ptr<RccGraph> graph,
    const function<void(int32_t, TxnOutput&)>& callback) {
  bool skip_graph = IsGraphOrphan(*graph, cmd_id);
  verify(0);
  verify(rpc_par_proxies_.find(par_id) != rpc_par_proxies_.end());
  for (auto& p : rpc_par_proxies_[par_id]) {
    auto proxy = (p.second);
    verify(proxy != nullptr);
    FutureAttr fuattr;
    fuattr.callback = [callback](Future* fu) {
      int32_t res;
      TxnOutput output;
      fu->get_reply() >> res >> output;
      callback(res, output);
    };
    verify(cmd_id > 0);
    if (skip_graph) {
      Future::safe_release(
          proxy->async_JanusCommitWoGraph(cmd_id, 0, need_validation, fuattr));
    } else {
      MarshallDeputy md(graph);
      Future::safe_release(
          proxy->async_JanusCommit(cmd_id, 0, need_validation, md, fuattr));
    }
  }
}

shared_ptr<QuorumEvent> JanusCommo::BroadcastInquireValidation(set<parid_t>& pars, txid_t txid) {
  auto e = Reactor::CreateSpEvent<QuorumEvent>(pars.size(), pars.size());
  for (auto par_id : pars) {
    auto proxy = NearestProxyForPartition(par_id).second;
    FutureAttr fuattr;
    fuattr.callback = [e](Future* fu) {
      int32_t res;
      fu->get_reply() >> res;
      if (res == 1) {
        e->VoteYes();
      } else if (res == -1) {
        e->VoteNo();
      } else {
        verify(0);
      }
    };
    verify(0);
    int rank = RANK_D;
    Future::safe_release(proxy->async_RccInquireValidation(txid, rank, fuattr));
  }
  return e;
}
void JanusCommo::BroadcastNotifyValidation(txid_t txid, set<parid_t>& pars, int32_t result) {
  for (auto par_id : pars) {
    for (auto pair : rpc_par_proxies_[par_id]) {
      auto proxy = pair.second;
      FutureAttr fuattr;
      fuattr.callback = [](Future* fu) {};
      int rank = RANK_D;
      verify(0);
      Future::safe_release(proxy->async_RccNotifyGlobalValidation(txid, rank, result, fuattr));
    }
  }

}

} // namespace janus
