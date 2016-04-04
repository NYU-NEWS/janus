#include <deptran/txn_reg.h>
#include "../config.h"
#include "../multi_value.h"
#include "../rcc/dep_graph.h"
#include "../rcc/graph_marshaler.h"
#include "sched.h"
#include "../tpl/exec.h"

namespace rococo {

int NoneSched::OnDispatch(const SimpleCommand &cmd,
                          rrr::i32 *res,
                          map<int32_t, Value> *output,
                          const function<void()>& callback) {
  auto dtxn = GetOrCreateDTxn(cmd.root_id_);
  txn_reg_->get(cmd).txn_handler(nullptr,
                                 dtxn,
                                 const_cast<SimpleCommand&>(cmd),
                                 res,
                                 *output);
  *res = SUCCESS;
  callback();
  return 0;
}

} // namespace rococo