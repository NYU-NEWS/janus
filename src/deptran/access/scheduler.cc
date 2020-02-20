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
        // first iterate through ssid_accessed --> has dedup'ed read/write on the same row-col
        for (const auto& row_col : acc_txn->sg.metadata.ssid_accessed) {
            for (const auto& col_ssid : row_col.second) {
                // update metadata for consistency check later on
                acc_txn->sg.update_metadata(col_ssid.second.ssid_low, col_ssid.second.ssid_high);
                // fill in ssid_highs for validation later
                acc_txn->sg.metadata.ssid_highs[row_col.first][col_ssid.first] = col_ssid.second.ssid_high;
            }
        }
        acc_txn->sg.metadata.ssid_accessed.clear();  // no need anymore, only for dispatch phase
        // fill return values
        *ssid_low = acc_txn->sg.metadata.highest_ssid_low;
        *ssid_high = acc_txn->sg.metadata.lowest_ssid_high;
        *ssid_highest = acc_txn->sg.metadata.highest_ssid_high;
        return acc_txn->sg.metadata.validate_abort;
    }

    void SchedulerAcc::OnValidate(cmdid_t cmd_id, snapshotid_t ssid_new, int8_t *res) {
        auto acc_txn = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(cmd_id));  // get the txn
        for (auto& row_col : acc_txn->sg.metadata.ssid_highs) {
            auto acc_row = dynamic_cast<AccRow*>(row_col.first);
            for (auto& col_ssid : row_col.second) {
                if (!acc_row->validate(col_ssid.first, col_ssid.second, ssid_new)) {
                    // validation fails on this row-col
                    acc_txn->sg.metadata.ssid_highs.clear();
                    *res = INCONSISTENT;
                    return;
                }
            }
        }
        acc_txn->sg.metadata.ssid_highs.clear();
        *res = CONSISTENT;
   }

    void SchedulerAcc::OnFinalize(cmdid_t cmd_id, int8_t decision) {
        auto acc_txn = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(cmd_id));  // get the txn
        for (auto& row_col : acc_txn->sg.metadata.pending_writes) {
            auto acc_row = dynamic_cast<AccRow*>(row_col.first);
            for (auto& col_index : row_col.second) {
                acc_row->finalize(col_index.first, col_index.second, decision);
            }
        }
        acc_txn->sg.metadata.pending_writes.clear();
    }
}