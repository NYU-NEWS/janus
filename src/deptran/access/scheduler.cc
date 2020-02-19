#include "scheduler.h"
#include "tx.h"

namespace janus {
    bool SchedulerAcc::OnDispatch(cmdid_t cmd_id,
                                  const shared_ptr<Marshallable>& cmd,
                                  uint64_t *ssid_low,
                                  uint64_t *ssid_high,
                                  uint64_t *ssid_highest,
                                  TxnOutput &ret_output) {
        // Step 1: dispatch and execute pieces
        auto sp_vec_piece = dynamic_pointer_cast<VecPieceData>(cmd)->sp_vec_piece_data_;
        verify(sp_vec_piece);
        auto tx = GetOrCreateTx(cmd_id);
        if (!tx->cmd_) {
            tx->cmd_ = cmd;
        } else {
            auto present_cmd = dynamic_pointer_cast<VecPieceData>(tx->cmd_)->sp_vec_piece_data_;
            for (auto& sp_piece_data : *sp_vec_piece) {
                present_cmd->push_back(sp_piece_data);
            }
        }
        for (const auto& sp_piece_data : *sp_vec_piece) {
            SchedulerClassic::ExecutePiece(*tx, *sp_piece_data, ret_output);
        }
        if (tx->fully_dispatched_->value_ == 0) {
            tx->fully_dispatched_->Set(1);
        }
        // Step 2: do local safeguard consistency check.
        auto acc_txn = dynamic_pointer_cast<AccTxn>(tx);
        // fill return values
        *ssid_low = acc_txn->sg.metadata.highest_ssid_low;
        *ssid_high = acc_txn->sg.metadata.lowest_ssid_high;
        *ssid_highest = acc_txn->sg.metadata.highest_ssid_high;
        return acc_txn->sg.metadata.validate_abort;
    }

    void SchedulerAcc::OnValidate(cmdid_t cmd_id, snapshotid_t ssid_new, int8_t *res) {
        // TODO: fill real logic in
        *res = CONSISTENT;
    }
}