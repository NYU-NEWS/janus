#include "scheduler.h"
#include "tx.h"
#include "predictor.h"
#include "bench/facebook/workload.h"
#include "bench/tpcc/workload.h"

namespace janus {
    int32_t SchedulerAcc::OnDispatch(cmdid_t cmd_id,
                                     const shared_ptr<Marshallable>& cmd,
                                     uint64_t ssid_spec,
                                     uint8_t single_shard,
                                     uint8_t write_only,
                                     uint64_t *ssid_low,
                                     uint64_t *ssid_high,
                                     uint64_t *ssid_new,
                                     TxnOutput &ret_output,
                                     uint64_t* arrival_time) {
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
        // get all the being-accessed row_keys, feed in predictor, and find decision
        uint64_t now = Predictor::get_current_time();  // the phyiscal arrival time of this tx
        *arrival_time = now;  // return arrival time in response
        //Log_info("server:OnDispatch. txid = %lu. ssid_spec = %lu.eturning arrival_time = %lu.", tx->tid_, ssid_spec, *arrival_time);
        bool will_block = false;
        // we always train a new tx even if it will be early-aborted
        //if (ssid_spec != 0) { // client-side ssid_spec logic is on
        if (false) {  // for testing purpose, disable server-side ML engine
            uint8_t workload;  // either FB, TPCC, or Spanner for now
            switch (sp_vec_piece->at(0)->root_type_) {
                case FB_ROTXN:
                case FB_WRITE:
                    workload = FACEBOOK;
                    break;
                    // case SPANNER:
                    //    break;
                default:  // TPCC
                    workload = TPCC;
                    break;
            }
            std::unordered_map<i32, optype_t> keys_to_train;
            for (const auto& sp_piece_data : *sp_vec_piece) {
                //Log_info("piece.op_type_ = %d.", sp_piece_data->op_type_);
                for (auto var_id : sp_piece_data->input.keys_) {
                    // in FB workloads, it's guaranteed that each input_ is a row key
                    i32 row_key = get_row_key(sp_piece_data, var_id, workload);
                    if (row_key == NOT_ROW_KEY) {
                        continue;
                    }
                    if (keys_to_train.find(row_key) == keys_to_train.end() || sp_piece_data->op_type_ == WRITE_REQ) {
                        keys_to_train[row_key] = sp_piece_data->op_type_;
                    }
                }
            }
            for (const auto& key_op : keys_to_train) {
                if (Predictor::should_block(key_op.first, now, ssid_spec, key_op.second)) {
                    will_block = true;
                }
            }
            keys_to_train.clear();
        }
        // deal with early-abort and single-shard, write-only txn
        if (single_shard) {
            tx->sg.disable_early_abort = true;
            // Log_info("Disabled early abort due to single-shard txn");
        }
        if (single_shard && write_only) {
            tx->sg.mark_finalized = true;
            // Log_info("Mark all writes finalized due to the txn is write only and single shard");
        }
        if (tx->sg.abort) { // early abort
            verify(!tx->sg.disable_early_abort);
            *ssid_low = 0;
            *ssid_high = 0;
            *ssid_new = 0;
            // *arrival_time = Predictor::get_current_time();
            for (const auto& sp_piece_data : *sp_vec_piece) {
                // prepare dummy ret_output
                ret_output[sp_piece_data->inn_id()] = {};
            }
            return EARLY_ABORT;
        }
        // now execute this txn
        // will_block = true; // todo: testing purpose, take this out later
        if (will_block && ssid_spec > now) {
            // wait until ssid_spec fires
            // TODO: heuristics or ML tells us how long it should wait
            uint64_t block_time = ssid_spec - now;
            if (block_time > MAX_BLOCK_TIMEOUT) {
                block_time = MAX_BLOCK_TIMEOUT;
            }
            Reactor::CreateSpEvent<NeverEvent>()->Wait(block_time);
        }
        tx->acc_query_start->Set(1);  // acc_query RPC can be handled only after we have executed this tx
        // Log_debug("Received AccDispatch for txnid = %lu; #pieces = %d.", tx->tid_, sp_vec_piece->size());
        for (const auto& sp_piece_data : *sp_vec_piece) {
            /*
            Log_debug("piece input size = %d. key size = %d.",
                    sp_piece_data->input.size(), sp_piece_data->input.keys_.size());
            for (const auto& key : sp_piece_data->input.keys_) {
                Log_debug("piece input id = %d; row key = %d.", key, sp_piece_data->input.at(key).get_i32());
            }
            */
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
        if (tx->sg.abort) {
            return EARLY_ABORT;
        }
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
	    // TODO: here we do async lambda callback that allows reads to wait for dependent versions to finalize
        // TODO: BUT how do we deal the deadlock case: w-->r/ w-->r ?
        // -----------------------------
        acc_txn->sg.validate_done = true;
        bool validate_consistent = true;
        for (auto& row_col : acc_txn->sg.metadata.indices) {
            auto acc_row = dynamic_cast<AccRow*>(row_col.first);
            for (auto& col_ssid : row_col.second) {
                if (!acc_row->validate(acc_txn->tid_, col_ssid.first, col_ssid.second, ssid_new, validate_consistent)) {
                    // validation fails on this row-col
                    validate_consistent = false;
                    // we need to go thru all records for possible early aborts
                }
            }
        }
	    *res = validate_consistent ? CONSISTENT : INCONSISTENT;
    }

    void SchedulerAcc::OnFinalize(cmdid_t cmd_id, int8_t decision) {
        auto acc_txn = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(cmd_id));  // get the txn
        //Log_info("parid = %d; txnid = %lu; Finalizing RPC. decision = %d.", this->partition_id_, acc_txn->tid_, decision);
        if (acc_txn->sg.metadata.indices.empty()) {
            // we've done finalize for this txn already
            //Log_info("txnid = %lu; Finalizing RPC. decision = %d. INDICES EMPTY, returned.", acc_txn->tid_, decision);
            return;
        }
        // for now, we do not update ssid for offset optimization case.
        for (auto& row_col : acc_txn->sg.metadata.indices) {
            auto acc_row = dynamic_cast<AccRow*>(row_col.first);
            for (auto& col_index : row_col.second) {
                acc_row->finalize(acc_txn->tid_, col_index.first, col_index.second, decision);
            }
        }
        // now we do AccQueryStatus callbacks
        if (acc_txn->query_callbacks.empty()) {
            // no later reads read this write
            return;
        }
        for (auto& callback : acc_txn->query_callbacks) {
            verify(0);
            callback(decision);
        }
        // clear metadata after finalizing a txn
        acc_txn->sg.reset_safeguard();
        acc_txn->query_callbacks.clear();
    }

    void SchedulerAcc::OnStatusQuery(cmdid_t cmd_id, int8_t *res, DeferredReply* defer) {
        auto acc_txn = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(cmd_id));  // get the txn
        // wait until acc_query_start is set 1 by OnAccDispatch
        acc_txn->acc_query_start->Wait(MAX_QUERY_TIMEOUT);
        acc_txn->acc_query_start->Set(0);  // reset
	    if (acc_txn->sg.metadata.reads_for_query.empty() || acc_txn->is_status_abort()) {
            *res = FINALIZED;	// some rpc has returned abort if is_status_abort
            verify(defer != nullptr);
            defer->reply();
            return;
        }
        // do a round of check in case there is validation or finalize arrive in between
        bool is_decided = true, will_abort = false;
        check_status(cmd_id, is_decided, will_abort);
        if (is_decided) {
            acc_txn->sg.metadata.reads_for_query.clear();
            // acc_txn->sg.status_query_done = true;
            *res = will_abort ? ABORTED : FINALIZED;
            defer->reply();
            return;
        }
        // now we check each pending version, and insert a callback func to that version waiting for its status
	    verify(!acc_txn->sg.metadata.reads_for_query.empty());
	    int rpc_id = acc_txn->n_query_inc();  // the rpc that needs later reply
        acc_txn->subrpc_count[rpc_id] = 0;
        acc_txn->subrpc_status[rpc_id] = FINALIZED;
        for (auto& row_col : acc_txn->sg.metadata.reads_for_query) {
            auto acc_row = dynamic_cast<AccRow *>(row_col.first);
            for (auto &col_index : row_col.second) {
		        if (col_index.second == 0) {
                    // recently decided
                    continue;
		        }
		        // acc_txn->n_query_inc();
		        acc_txn->subrpc_count[rpc_id]++;
		        txnid_t to_tid = acc_row->get_ver_tid(col_index.first, col_index.second);  // inserting the callback of current txn to txn:tid
		        auto to_txn = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(to_tid));  // get the target txn
		        to_txn->insert_callbacks(acc_txn, res, defer, rpc_id);
            }
        }
        acc_txn->sg.metadata.reads_for_query.clear(); // no need those records anymore, might add more by later dispatch
    }

    void SchedulerAcc::check_status(cmdid_t cmd_id, bool& is_decided, bool& will_abort) {
        auto acc_txn = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(cmd_id));  // get the txn
        for (auto& row_col : acc_txn->sg.metadata.reads_for_query) {
            auto acc_row = dynamic_cast<AccRow *>(row_col.first);
            for (auto &col_index : row_col.second) {
                int8_t status = acc_row->check_status(col_index.first, col_index.second);
                switch (status) {
                    case UNCHECKED:
                    case VALIDATING:
                        is_decided = false;
                        break;
                    case FINALIZED:
                        col_index.second = 0;
			//acc_txn->sg.metadata.reads_for_query[row_col.first].erase(col_index.first);
                        break;
                    case ABORTED:
			            col_index.second = 0;
                        //acc_txn->sg.metadata.reads_for_query[row_col.first].erase(col_index.first);
                        will_abort = true;
                        break;
                }
            }
        }
    }

    i32 SchedulerAcc::get_row_key(const shared_ptr<SimpleCommand> &sp_piece_data, int32_t var_id, uint8_t workload) const {
        switch (workload) {
            case FACEBOOK:
                // in FB workloads, it's guaranteed that each input_ is a row key
                return sp_piece_data->input.at(var_id).get_i32();
            case TPCC: {
                // more complicated
                if (!tpcc_row_key(var_id)) {
                    return NOT_ROW_KEY;
                }
                if (sp_piece_data->input.values_->find(var_id) == sp_piece_data->input.values_->end()) {
                    // key/value dependent, not ready yet
                    return NOT_ROW_KEY;
                }
                Value k = sp_piece_data->input.at(var_id);
                if (k.get_kind() != mdb::Value::I32) {
                    // tpcc, either a string: by_last (not really readcolumn or writecolumn) or unfilled key
                    // or a double used as col values not row identifiers
                    return NOT_ROW_KEY;
                } else {
                    return sp_piece_data->input.at(var_id).get_i32();
                }
            }
            default: verify(0);
                return NOT_ROW_KEY;
        }
    }

    bool SchedulerAcc::tpcc_row_key(int32_t var_id) const {
        return (var_id == TPCC_VAR_C_ID ||
                var_id == TPCC_VAR_D_ID ||
                var_id == TPCC_VAR_W_ID ||
                var_id == TPCC_VAR_O_ID ||
                var_id == TPCC_VAR_C_D_ID ||
                var_id == TPCC_VAR_C_W_ID ||
                (TPCC_VAR_I_ID(0) <= var_id && var_id < TPCC_VAR_I_NAME(0)) ||
                (TPCC_VAR_S_W_ID(0) <= var_id && var_id < TPCC_VAR_S_D_ID(0)) ||
                TPCC_VAR_OL_I_ID(0) <= var_id);
    }
}
