

#include "../config.h"
#include "../multi_value.h"
#include "../rcc/dep_graph.h"
#include "../rcc/graph_marshaler.h"
#include "sched.h"
#include "../tpl/exec.h"

namespace rococo {

int NoneSched::OnPhaseOneRequest(const SimpleCommand& cmd,
                                 rrr::i32 *res,
                                 map<int32_t, Value> *output,
                                 rrr::DeferredReply *defer) {
  TPLExecutor *exec = (TPLExecutor*) GetOrCreateExecutor(cmd.root_id_);
  exec->execute(cmd, res, *output);
  defer->reply();
  return 0;
}

} // namespace rococo