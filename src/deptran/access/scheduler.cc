#include "scheduler.h"
#include "tx.h"

namespace janus {
    int32_t SchedulerAcc::OnDispatch(cmdid_t cmd_id,
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
        // Step 2: report ssid ranges
        auto acc_txn = dynamic_pointer_cast<AccTxn>(tx);
        // fill return values
        *ssid_low = acc_txn->sg.metadata.highest_ssid_low;
        *ssid_high = acc_txn->sg.metadata.lowest_ssid_high;
        *ssid_highest = acc_txn->sg.metadata.highest_ssid_high;
        if (acc_txn->sg.metadata.validate_abort) {
            return VALIDATE_ABORT;
        }
        if (!acc_txn->sg.offset_1_valid) {
            return OFFSET_INVALID;
        }
        return SUCCESS;
    }

    void SchedulerAcc::OnValidate(cmdid_t cmd_id, snapshotid_t ssid_new, int8_t *res) {
        auto acc_txn = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(cmd_id));  // get the txn
        if (acc_txn->sg.validate_done) {
            // multiple pieces may share the same scheduler and thus validate on the same indices map
            *res = CONSISTENT;  // if there is at least one validation fails, final result will be fail
            return;
        }
        acc_txn->sg.validate_done = true;
        for (auto& row_col : acc_txn->sg.metadata.indices) {
            auto acc_row = dynamic_cast<AccRow*>(row_col.first);
            for (auto& col_ssid : row_col.second) {
                if (!acc_row->validate(col_ssid.first, col_ssid.second, ssid_new)) {
                    // validation fails on this row-col
                    *res = INCONSISTENT;
                    return;
                }
            }
        }
        *res = CONSISTENT;
   }

    void SchedulerAcc::OnFinalize(cmdid_t cmd_id, int8_t decision, snapshotid_t ssid_new) {
        auto acc_txn = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(cmd_id));  // get the txn
        if (acc_txn->sg.metadata.indices.empty()) {
            // we've done finalize for this txn already
            return;
        }
        if (acc_txn->sg.validate_done) {
            // this txn did validation earlier, so ssid highs have been updated already
            ssid_new = 0;
        }
        // we might need to update ssid in finalize for the ssid-diff=1 special case (optimization)
        for (auto& row_col : acc_txn->sg.metadata.indices) {
            auto acc_row = dynamic_cast<AccRow*>(row_col.first);
            for (auto& col_index : row_col.second) {
                acc_row->finalize(col_index.first, col_index.second, decision, ssid_new);
            }
        }
        // clear metadata after finalizing a txn
        acc_txn->sg.reset_safeguard();
    }
}