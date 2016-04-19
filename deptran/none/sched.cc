#include <deptran/txn_reg.h>
#include "../config.h"
#include "../multi_value.h"
#include "../rcc/dep_graph.h"
#include "../txn_chopper.h"
#include "../rcc/graph_marshaler.h"
#include "sched.h"
#include "../tpl/exec.h"

namespace rococo {

int NoneSched::OnDispatch(const vector<SimpleCommand> &cmd,
                          rrr::i32 *res,
                          TxnOutput *output,
                          const function<void()>& callback) {
  auto dtxn = GetOrCreateDTxn(cmd[0].root_id_);
  verify(partition_id_ == cmd[0].partition_id_);
  for (auto& c : cmd) {
    txn_reg_->get(c).txn_handler(nullptr,
                                 dtxn,
                                 const_cast<SimpleCommand&>(c),
                                 res,
                                 (*output)[c.inn_id()]);
  }

  *res = SUCCESS;
  callback();
  return 0;
}

} // namespace rococo