#include "commo.h"

using namespace janus;

// TODO change the event reference to weak ptr? because the event could be
// destroyed before accessed if a quorum is satisfied early.
void CommoFebruus::BroadcastPreAccept(QuorumEvent& e,
                                      parid_t par_id,
                                      txid_t tx_id) {
  verify(rpc_par_proxies_.find(par_id) != rpc_par_proxies_.end());

  for (auto& p : rpc_par_proxies_[par_id]) {
    auto proxy = (p.second);
    verify(proxy != nullptr);
    FutureAttr fuattr;
    fuattr.callback = [&e](Future* fu) {
      int32_t res;
      uint64_t timestamp;
      fu->get_reply() >> res >> timestamp;
      e.vec_timestamp_.push_back(timestamp);
      e.VoteYes();
    };
    verify(tx_id > 0);
    Future::safe_release(proxy->async_PreAcceptFebruus(tx_id, fuattr));
  }
}

void CommoFebruus::BroadcastAccept(QuorumEvent& e,
                                   parid_t par_id,
                                   txid_t tx_id,
                                   ballot_t ballot,
                                   uint64_t timestamp) {
  verify(rpc_par_proxies_.find(par_id) != rpc_par_proxies_.end());

  for (auto& p : rpc_par_proxies_[par_id]) {
    auto proxy = (p.second);
    verify(proxy != nullptr);
    FutureAttr fuattr;
    fuattr.callback = [&e](Future* fu) {
      int32_t res;
      uint64_t timestamp;
      fu->get_reply() >> res;
      e.VoteYes();
    };
    verify(tx_id > 0);
    auto f = proxy->async_AcceptFebruus(tx_id, ballot, timestamp, fuattr);
    Future::safe_release(f);
  }
}

void CommoFebruus::BroadcastCommit(const set<parid_t>& set_par_id,
                                   txid_t tx_id,
                                   uint64_t timestamp) {
  for (auto par_id : set_par_id) {
    verify(rpc_par_proxies_.find(par_id) != rpc_par_proxies_.end());
    for (auto& p : rpc_par_proxies_[par_id]) {
      auto proxy = (p.second);
      verify(proxy != nullptr);
      FutureAttr fuattr;
      fuattr.callback = [](Future* fu) {
        int32_t res;
        fu->get_reply() >> res;
      };
      verify(tx_id > 0);

      auto f = proxy->async_CommitFebruus(tx_id, timestamp, fuattr);
      Future::safe_release(f);
    }
  }
}
