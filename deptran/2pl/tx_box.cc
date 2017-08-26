
#include "tx_box.h"
#include "scheduler.h"

namespace janus {

TplTxBox::TplTxBox(epoch_t epoch,
                 txnid_t tid,
                 Scheduler *sched) : TxBox(epoch, tid, sched) {

}

} // namespace janus
