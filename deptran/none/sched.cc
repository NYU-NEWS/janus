

#include "../config.h"
#include "../multi_value.h"
#include "../rcc/dep_graph.h"
#include "../rcc/graph_marshaler.h"
#include "sched.h"
#include "../tpl/exec.h"

namespace rococo {

int NoneSched::OnHandoutRequest(const SimpleCommand &cmd,
                                rrr::i32 *res,
                                map<int32_t, Value> *output,
                                rrr::DeferredReply *defer) {
  auto exec = GetOrCreateExecutor(cmd.root_id_);
  exec->Execute(cmd, res, *output);
  defer->reply();
  return 0;
}

} // namespace rococo