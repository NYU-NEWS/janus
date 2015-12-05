
#include "sched.h"
#include "../tpl/tpl.h"

namespace rococo {

int NoneSched::OnPhaseOneRequest(
    const RequestHeader &header,
    const std::vector<mdb::Value> &input,
    const rrr::i32 &output_size,
    rrr::i32 *res,
    std::vector<mdb::Value> *output,
    rrr::DeferredReply *defer) {
  TPLDTxn* dtxn = (TPLDTxn*) this->GetOrCreate(header.tid);
  dtxn->execute(header, input, res, output);
  defer->reply();
  return 0;
}

} // namespace rococo