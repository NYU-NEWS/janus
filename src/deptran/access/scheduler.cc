#include "scheduler.h"
#include "tx.h"

namespace janus {
    int32_t SchedulerAcc::OnDispatch(cmdid_t cmd_id,
                                     const shared_ptr<Marshallable>& cmd,
                                     uint64_t ssid_spec,
                                     uint64_t *ssid_low,
                                     uint64_t *ssid_high,
                                     uint64_t *ssid_new,
                                     TxnOutput &ret_output) {
        // Step 1: dispatch and execute pieces
        auto sp_vec_piece = dynamic_pointer_cast<VecPieceData>(cmd)->sp_vec_piece_data_;
        verify(sp_vec_piece);
        //auto tx = GetOrCreateTx(cmd_id);
        auto tx = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(cmd_id));
        tx->load_speculative_ssid(ssid_spec);   // read in the spec ssid provided by the client
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
        // Step 2: report ssid status
        *ssid_low = tx->sg.metadata.highest_ssid_low;
        *ssid_high = tx->sg.metadata.lowest_ssid_high;
        *ssid_new = tx->sg.metadata.highest_write_ssid;
        // report offset_invalid and decided. These two things are *incomparable*!
        if (!tx->sg.decided && !tx->sg.offset_safe) {
            return BOTH_NEGATIVE;
        }
        if (!tx->sg.decided) {
            return NOT_DECIDED;
        }
        if (!tx->sg.offset_safe) {
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
        acc_txn->sg.decided = true;   // clear decided from dispatch
        acc_txn->sg.validate_done = true;
        bool validate_consistent = true;
        for (auto& row_col : acc_txn->sg.metadata.indices) {
            auto acc_row = dynamic_cast<AccRow*>(row_col.first);
            for (auto& col_ssid : row_col.second) {
                if (!acc_row->validate(acc_txn->tid_, col_ssid.first, col_ssid.second, ssid_new, validate_consistent, acc_txn->sg.decided)) {
                    // validation fails on this row-col
                    validate_consistent = false;
                    // we need to go thru all records for possible early aborts
                }
            }
        }
        if (!validate_consistent) {
            *res = INCONSISTENT;
        } else if (!acc_txn->sg.decided) {
            *res = NOT_DECIDED;  // not decided but consistent
        } else {
            *res = CONSISTENT;   // consistent and decided
        }
    }

    void SchedulerAcc::OnFinalize(cmdid_t cmd_id, int8_t decision) {
        auto acc_txn = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(cmd_id));  // get the txn
        if (acc_txn->sg.metadata.indices.empty()) {
            // we've done finalize for this txn already
            return;
        }
        // for now, we do not update ssid for offset optimization case.
        for (auto& row_col : acc_txn->sg.metadata.indices) {
            auto acc_row = dynamic_cast<AccRow*>(row_col.first);
            for (auto& col_index : row_col.second) {
                acc_row->finalize(acc_txn->tid_, col_index.first, col_index.second, decision);
            }
        }
        // clear metadata after finalizing a txn
        acc_txn->sg.reset_safeguard();
    }

    void SchedulerAcc::OnStatusQuery(cmdid_t cmd_id, int8_t *res, DeferredReply* defer) {
        auto acc_txn = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(cmd_id));  // get the txn
        if (acc_txn->sg.status_query_done || acc_txn->sg.decided) {
            // 1. if we send 1 rpc per shard then we don't need sg.status_query_done
            // 2. if sg.decided is true then we know all versions are decided because StatusQuery RPC
            // arrives after dispatch --> during dispatch we know all versions are decided. THIS NEEDS 1.
            *res = FINALIZED;   // will be aborted if any rpc returns abort
            acc_txn->sg.status_query_done = true;
            verify(defer != nullptr);
            defer->reply();
            return;
        }
        // now we check each pending version, and insert a callback func to that version waiting for its status
        for (auto& row_col : acc_txn->sg.metadata.indices) {
            auto acc_row = dynamic_cast<AccRow *>(row_col.first);
            for (auto &col_index : row_col.second) {
                acc_row->register_query_callback(acc_txn, col_index.first, col_index.second, res, defer);
            }
        }
    }
}
