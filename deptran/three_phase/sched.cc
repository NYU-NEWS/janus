
#include "../constants.h"
#include "../dtxn.h"
#include "sched.h"
#include "../tpl/tpl.h"

namespace rococo {
int ThreePhaseSched::OnPhaseOneRequest(
    const RequestHeader &header,
    const std::vector<mdb::Value> &input,
    const rrr::i32 &output_size,
    rrr::i32 *res,
    std::vector<mdb::Value> *output,
    rrr::DeferredReply *defer) {
  TPLDTxn* dtxn = (TPLDTxn*)this->GetOrCreate(header.tid);
  if (IS_MODE_2PL) {
    verify(dtxn->mdb_txn_->rtti() == mdb::symbol_t::TXN_2PL);
    DragonBall *defer_reply_db = new DragonBall(1, [defer, res]() {
      defer->reply();
    });
    dtxn->pre_execute_2pl(header, input, res, output, defer_reply_db);
  } else if (IS_MODE_NONE) {
    dtxn->execute(header, input, res, output);
    defer->reply();

  } else if (IS_MODE_OCC) {
    dtxn->execute(header, input, res, output);
    defer->reply();
  } else {
    verify(0);
  }
  return 0;
}
} // namespace rococo