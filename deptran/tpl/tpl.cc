
#include "tpl.h"
#include "scheduler.h"

namespace janus {

TplTxBox::TplTxBox(epoch_t epoch,
                 txnid_t tid,
                 Scheduler *sched) : DTxn(epoch, tid, sched) {

}

} // namespace janus
