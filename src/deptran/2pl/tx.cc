
#include "tx.h"
#include "scheduler.h"

namespace janus {

Tx2pl::Tx2pl(epoch_t epoch,
             txnid_t tid,
             TxLogServer *sched) : TxClassic(epoch, tid, sched) {

}

} // namespace janus
