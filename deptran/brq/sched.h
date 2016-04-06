
#pragma once
#include "../rcc/sched.h"

namespace rococo {

class RccGraph;
class BrqSched : public RccSched {
 public:
  using RccSched::RccSched;

  void OnPreAccept(const txnid_t txnid,
                   const RccGraph& graph,
                   int32_t* res,
                   RccGraph* res_graph,
                   function<void()> callback);

  void OnCommit(const txnid_t txn_id,
                const RccGraph& graph,
                int32_t* res,
                TxnOutput* output,
                function<void()> callback);
};

} // namespace rococo