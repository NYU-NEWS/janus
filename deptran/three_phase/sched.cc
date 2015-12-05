
#include "../constants.h"
#include "../dtxn.h"
#include "sched.h"
#include "../tpl/tpl.h"
#include "exec.h"

namespace rococo {


int ThreePhaseSched::OnPhaseTwoRequest(
    cmdid_t cmd_id,
    const std::vector <i32> &sids,
    rrr::i32 *res,
    rrr::DeferredReply *defer) {
  auto exec = (ThreePhaseExecutor*)GetExecutor(cmd_id);
  exec->prepare_launch(sids, res, defer);
  return 0;
}

int ThreePhaseSched::OnPhaseThreeRequest(
    cmdid_t cmd_id,
    int commit_or_abort,
    rrr::i32 *res,
    rrr::DeferredReply *defer
) {
  auto exec = (ThreePhaseExecutor*)GetExecutor(cmd_id);
  if (commit_or_abort == SUCCESS) {
    exec->commit_launch(res, defer);
  } else if (commit_or_abort == REJECT) {
    exec->abort_launch(res, defer);
  } else {
    verify(0);
  }
  return 0;
}


} // namespace rococo