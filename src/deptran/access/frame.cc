#include "frame.h"
#include "coordinator.h"
#include "scheduler.h"
#include "row.h"
#include "tx.h"

namespace janus {
    REG_FRAME(MODE_ACC, vector<string>({"acc"}), FrameAcc);
    Coordinator* FrameAcc::CreateCoordinator(cooid_t coo_id,
                                             Config *config,
                                             int benchmark,
                                             janus::ClientControlServiceImpl *ccsi,
                                             uint32_t id,
                                             shared_ptr<TxnRegistry> txn_reg) {
        // verify(config != nullptr);
        auto *coord = new CoordinatorAcc(coo_id,
                                         benchmark,
                                         ccsi,
                                         id);
        coord->txn_reg_ = txn_reg;
        coord->frame_ = this;
        return coord;
    }

    TxLogServer* FrameAcc::CreateScheduler() {
        TxLogServer *sched = new SchedulerAcc();
        sched->frame_ = this;
        return sched;
    }

    mdb::Row* FrameAcc::CreateRow(const mdb::Schema *schema,
                                  vector<Value> &row_data) {
        // verify(row_data.size() == schema->columns_count()); // workloads may only populate a subset of cols, e.g., FB
        return (mdb::Row*)AccRow::create(schema, row_data);
    }

    shared_ptr<Tx> FrameAcc::CreateTx(epoch_t epoch, txnid_t tid,
                                      bool ro, TxLogServer *mgr) {
        shared_ptr<Tx> sp_tx(new AccTxn(epoch, tid, mgr));
        return sp_tx;
    }

    Communicator* FrameAcc::CreateCommo(PollMgr *pollmgr) {
        commo_ = new AccCommo(pollmgr);
        return commo_;
    }
}