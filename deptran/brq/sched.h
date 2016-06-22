
#pragma once
#include "../rcc/sched.h"

namespace rococo {

class RccGraph;
class BrqCommo;
class BrqSched : public RccSched {
 public:
  using RccSched::RccSched;

  void OnPreAccept(const txnid_t txnid,
                   const vector<SimpleCommand> &cmds,
                   const RccGraph &graph,
                   int32_t *res,
                   RccGraph *res_graph,
                   function<void()> callback);

  void OnPreAcceptWoGraph(const txnid_t txnid,
                          const vector<SimpleCommand> &cmds,
                          int32_t *res,
                          RccGraph *res_graph,
                          function<void()> callback);

  void OnAccept(const txnid_t txn_id,
                const ballot_t& ballot,
                const RccGraph& graph,
                int32_t* res,
                function<void()> callback);

  void OnCommit(const txnid_t txn_id,
                const RccGraph &graph,
                int32_t *res,
                TxnOutput *output,
                const function<void()> &callback);

  void OnCommitWoGraph(const txnid_t cmd_id,
                       int32_t* res,
                       TxnOutput* output,
                       const function<void()>& callback);

  int OnInquire(cmdid_t cmd_id,
                RccGraph *graph,
                const function<void()> &callback) override;
  BrqCommo* commo();

  void TriggerUpgradeEpoch();
  void UpgradeEpochAck(parid_t par_id, siteid_t site_id, int res);
  void TriggerTruncateEpoch();
  int32_t OnUpgradeEpoch(uint32_t old_epoch) {
    // TODO
    if (old_epoch == curr_epoch_) {
      curr_epoch_++;
      return 0;
    } else if (curr_epoch_ > old_epoch) {
      return 1;
    } else {
      return -1;
    }
  }
};
} // namespace rococo