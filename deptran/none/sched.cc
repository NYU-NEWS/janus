

#include "../config.h"
#include "../multi_value.h"
#include "../rcc/dep_graph.h"
#include "../rcc/graph_marshaler.h"
#include "sched.h"
#include "../tpl/exec.h"

namespace rococo {

int NoneSched::OnPhaseOneRequest(
    const RequestHeader &header,
    const std::vector<mdb::Value> &input,
    const rrr::i32 &output_size,
    rrr::i32 *res,
    std::vector<mdb::Value> *output,
    rrr::DeferredReply *defer) {
  TPLExecutor *exec = (TPLExecutor*) GetOrCreateExecutor(header.tid);
  exec->execute(header, input, res, output);
  defer->reply();
  return 0;
}

} // namespace rococo