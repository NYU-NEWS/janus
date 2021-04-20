#include "scheduler.h"
#include "tx.h"
#include "predictor.h"
#include "bench/facebook/workload.h"
#include "bench/tpcc/workload.h"
#include "bench/spanner/workload.h"
#include "bench/dynamic/workload.h"

namespace janus {
    uint64_t SchedulerAcc::max_write_ts = 0;

    AccCommo* SchedulerAcc::commo() {
        if (commo_ == nullptr) {
            commo_ = new AccCommo();
        }
        auto acc_commo = dynamic_cast<AccCommo*>(commo_);
        verify(acc_commo != nullptr);
        return acc_commo;
    }

    int32_t SchedulerAcc::OnDispatch(cmdid_t cmd_id,
                                     const shared_ptr<Marshallable>& cmd,
                                     uint64_t ssid_spec,
                                     uint64_t safe_ts,
                                     uint8_t is_single_shard_write_only,
                                     const uint32_t& coord,
                                     const std::unordered_set<uint32_t>& cohorts,
                                     uint64_t *ssid_low,
                                     uint64_t *ssid_high,
                                     uint64_t *ssid_new,
                                     TxnOutput &ret_output,
                                     uint64_t* arrival_time,
                                     uint8_t* rotxn_okay,
                                     std::pair<parid_t, uint64_t>* new_svr_ts) {
        // Step 1: dispatch and execute pieces
        auto sp_vec_piece = dynamic_pointer_cast<VecPieceData>(cmd)->sp_vec_piece_data_;
        // verify(sp_vec_piece);
        if (!sp_vec_piece) {
            return SUCCESS;
        }
        //auto tx = GetOrCreateTx(cmd_id);
        auto tx = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(cmd_id));
        // failure handling
        if (safe_ts == UINT64_MAX) {
            // this is a read-write txn
            tx->record.coord = coord;
            if (this->partition_id_ == coord) {
                // this partition is the backup coordinator
                tx->record.cohorts.insert(cohorts.begin(), cohorts.end());
            }
        }
        /* Disable HF for now
        // register handle_failure
        if (!tx->handle_failure) {
            tx->handle_failure = true;
            Coroutine::CreateRun([this, cmd_id]() {
                handle_failure(cmd_id, this->partition_id_);
            });
        }
        */

        /* test code
        if (this->partition_id_ != coord) {
            Log_info("this svr = %d, sending to coord = %d, with cmd_id = %lu.", this->partition_id_, coord, cmd_id);
            commo()->AccBroadcastResolveStatusCoord(coord,
                                                    cmd_id,
                                                    std::bind(&SchedulerAcc::AccResolveStatusCoordAck,
                                                              this,
                                                              cmd_id,
                                                              std::placeholders::_1));
        }

        if (this->partition_id_ == coord) {
            for (auto cohort : tx->record.cohorts) {
                Log_info("this svr = %d, is coord. sending to cohort = %d, with cmd_id = %lu.", this->partition_id_, cohort, cmd_id);
                commo()->AccBroadcastGetRecord(cohort,
                                               cmd_id,
                                               std::bind(&SchedulerAcc::AccGetRecordAck,
                                                         this,
                                                         cmd_id,
                                                         std::placeholders::_1,
                                                         std::placeholders::_2,
                                                         std::placeholders::_3));
            }
        }
        */

        tx->load_speculative_ssid(ssid_spec);   // read in the spec ssid provided by the client
        if (!tx->cmd_) {
            tx->cmd_ = cmd;
        } else {
            auto present_cmd = dynamic_pointer_cast<VecPieceData>(tx->cmd_)->sp_vec_piece_data_;
            for (auto& sp_piece_data : *sp_vec_piece) {
                present_cmd->push_back(sp_piece_data);
            }
        }
        /*
        string cs = "";
        for (auto c : cohorts) {
            cs += to_string(c);
            cs += " | ";
        }
        Log_info("parid = %d; txnid = %lu; OnDispatch. coord = %u. cohorts = %s.", this->partition_id_, tx->tid_, coord, cs.c_str());
        */
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
                case SPANNER_ROTXN:
                case SPANNER_RW:
                    workload = SPANNER;
                    break;
                case DYNAMIC_ROTXN:
                case DYNAMIC_RW:
                    workload = DYNAMIC;
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
        if (is_single_shard_write_only) {
            tx->sg.mark_finalized = true;
            // failure handling
            tx->record.status = FINALIZED;
            // Log_info("Mark all writes finalized due to the txn is write only and single shard");
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
        // tx->acc_query_start->Set(1);  // acc_query RPC can be handled only after we have executed this tx
        // Log_debug("Received AccDispatch for txnid = %lu; #pieces = %d.", tx->tid_, sp_vec_piece->size());
        for (const auto& sp_piece_data : *sp_vec_piece) {
            /*
            Log_debug("piece input size = %d. key size = %d.",
                    sp_piece_data->input.size(), sp_piece_data->input.keys_.size());
            for (const auto& key : sp_piece_data->input.keys_) {
                Log_debug("piece input id = %d; row key = %d.", key, sp_piece_data->input.at(key).get_i32());
            }
            */
            if (sp_piece_data->root_type_ == FB_ROTXN || sp_piece_data->root_type_ == SPANNER_ROTXN || sp_piece_data->root_type_ == TPCC_STOCK_LEVEL) {
                tx->is_rotxn = true;
                // i32 key = get_key(sp_piece_data);
                // uint64_t ts = sp_piece_data->input.at(TS_INDEX).get_i64();
                // tx->key_to_ts[key] = ts;
                tx->safe_ts = safe_ts;
                verify(safe_ts < UINT64_MAX);
            }
            SchedulerClassic::ExecutePiece(*tx, *sp_piece_data, ret_output);
        }
        if (tx->fully_dispatched_->value_ == 0) {
            tx->fully_dispatched_->Set(1);
        }
        // Log_info("parid = %d; txnid = %lu; indices size = %d.", this->partition_id_, tx->tid_, tx->sg.metadata.indices.size());
        // Step 2: report ssid status
        *ssid_low = tx->sg.metadata.highest_ssid_low;
        *ssid_high = tx->sg.metadata.lowest_ssid_high;
        *ssid_new = tx->sg.metadata.highest_write_ssid;

        // failure handling
        tx->record.ts_low = *ssid_low;
        tx->record.ts_high = *ssid_high;

        // rotxn
        *rotxn_okay = 1;
        if (sp_vec_piece->at(0)->root_type_ == FB_ROTXN || sp_vec_piece->at(0)->root_type_ == SPANNER_ROTXN || sp_vec_piece->at(0)->root_type_ == TPCC_STOCK_LEVEL) {
            if (!tx->rotxn_okay) {
                *rotxn_okay = 0;
            }
        }
        if (this->max_write_ts < tx->write_ts) {
            this->max_write_ts = tx->write_ts;
        }
        *new_svr_ts = std::make_pair(this->partition_id_, this->max_write_ts);

        // report offset_invalid and decided. These two things are *incomparable*!
        /*
        if (!tx->sg.decided && !tx->sg.offset_safe) {
            return BOTH_NEGATIVE;
        }
        */
        if (!tx->sg.decided) {
            return NOT_DECIDED;
        }
        // failure handling
        tx->record.status = CLEARED; // the txn is decided
        /*
        if (!tx->sg.offset_safe) {
            return OFFSET_INVALID;
        }
        */
        return SUCCESS;
    }

    void SchedulerAcc::OnValidate(cmdid_t cmd_id, snapshotid_t ssid_new, int8_t *res) {
        auto acc_txn = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(cmd_id));  // get the txn
        // Log_info("parid = %d; txnid = %lu; Received validation.", this->partition_id_, acc_txn->tid_);
        /// Log_info("parid = %d; txnid = %lu; Validation. status_before = %d, ssid_low = %lu, ssid_high = %lu.", this->partition_id_, acc_txn->tid_, acc_txn->record.status, acc_txn->record.ts_low, acc_txn->record.ts_high);
        if (acc_txn->sg.validate_done) {
            verify(0);  // should never get here
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
        // failure handling
        if (!validate_consistent) {
            // this txn will abort, we set record aborted now
            acc_txn->record.status = ABORTED;
        }
        /// Log_info("parid = %d; txnid = %lu; Validation. status_after = %d, ssid_low = %lu, ssid_high = %lu.", this->partition_id_, acc_txn->tid_, acc_txn->record.status, acc_txn->record.ts_low, acc_txn->record.ts_high);
	    *res = validate_consistent ? CONSISTENT : INCONSISTENT;
    }

    void SchedulerAcc::OnFinalize(cmdid_t cmd_id, int8_t decision) {
        auto acc_txn = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(cmd_id));  // get the txn
        /// Log_info("parid = %d; txnid = %lu; Finalize. status_before = %d, ssid_low = %lu, ssid_high = %lu.", this->partition_id_, acc_txn->tid_, acc_txn->record.status, acc_txn->record.ts_low, acc_txn->record.ts_high);
        // failure handling
        acc_txn->record.status = decision; // set status: either FINALIZED OR ABORTED
        /// Log_info("parid = %d; txnid = %lu; Finalize. status_after = %d, ssid_low = %lu, ssid_high = %lu.", this->partition_id_, acc_txn->tid_, acc_txn->record.status, acc_txn->record.ts_low, acc_txn->record.ts_high);
        // Log_info("parid = %d; txnid = %lu; Finalizing RPC. decision = %d.", this->partition_id_, acc_txn->tid_, decision);
        if (acc_txn->sg.metadata.indices.empty()) {
            // we've done finalize for this txn already
            // Log_info("txnid = %lu; Finalizing RPC. decision = %d. INDICES EMPTY, returned.", acc_txn->tid_, decision);
            // verify(0); // should not get here
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
            callback(decision);
        }
        // clear metadata after finalizing a txn
        acc_txn->sg.reset_safeguard();
        acc_txn->query_callbacks.clear();
    }

    void SchedulerAcc::OnStatusQuery(cmdid_t cmd_id, int8_t *res, DeferredReply* defer) {
        auto acc_txn = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(cmd_id));  // get the txn
        // Log_info("txnid = %lu; On statusQuery.", acc_txn->tid_);
        // wait until acc_query_start is set 1 by OnAccDispatch
        // acc_txn->acc_query_start->Wait(MAX_QUERY_TIMEOUT);
        // acc_txn->acc_query_start->Set(0);  // reset
        /// Log_info("parid = %d; txnid = %lu; StatusQuery. status_before = %d, ssid_low = %lu, ssid_high = %lu.", this->partition_id_, acc_txn->tid_, acc_txn->record.status, acc_txn->record.ts_low, acc_txn->record.ts_high);
	    if ((acc_txn->sg.metadata.reads_for_query.empty()
	        && acc_txn->sg.metadata.writes_for_query.empty())
	        || acc_txn->is_status_abort()) {
            *res = FINALIZED;	// some rpc has returned abort if is_status_abort
            // failure handling, this txn's querystate rpc has responded
            if (acc_txn->record.status == UNCLEARED) {
                acc_txn->record.status = CLEARED;
            }
            verify(defer != nullptr);
            defer->reply();
            return;
        }
        // do a round of check in case there is validation or finalize arrive in between
        bool is_decided = true, will_abort = false;
        check_status(cmd_id, is_decided, will_abort);
        if (is_decided) {
            acc_txn->sg.metadata.reads_for_query.clear();
            acc_txn->sg.metadata.writes_for_query.clear();
            // failure handling, this txn's querystate rpc has responded
            if (acc_txn->record.status == UNCLEARED) {
                acc_txn->record.status = CLEARED;
            }
            if (will_abort) {
                acc_txn->record.status = ABORTED;
            }

            // acc_txn->sg.status_query_done = true;
            *res = will_abort ? ABORTED : FINALIZED;
            defer->reply();
            return;
        }
        // now we check each pending version, and insert a callback func to that version waiting for its status
	    // verify(!acc_txn->sg.metadata.reads_for_query.empty() || !acc_txn->sg.metadata.writes_for_query.empty());
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
		        //acc_txn->n_query_inc();
		        acc_txn->subrpc_count[rpc_id]++;
		        txnid_t to_tid = acc_row->get_ver_tid(col_index.first, col_index.second);  // inserting the callback of current txn to txn:tid
		        auto to_txn = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(to_tid));  // get the target txn
		        to_txn->insert_callbacks(acc_txn, res, defer, rpc_id);
		        //int col_id = col_index.first, version = col_index.second;
                //acc_row->read_query_wait(col_id, version);
            }
        }
        for (auto& row_col : acc_txn->sg.metadata.writes_for_query) {
            auto acc_row = dynamic_cast<AccRow *>(row_col.first);
            for (auto &col_index : row_col.second) {
                if (col_index.second == -1) {
                    // recently decided
                    continue;
                }
                // acc_txn->n_query_inc();
                acc_txn->subrpc_count[rpc_id]++;
                //txnid_t to_tid = acc_row->get_ver_tid(col_index.first, col_index.second);  // inserting the callback of current txn to txn:tid
                //auto to_txn = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(to_tid));  // get the target txn
                acc_row->insert_write_callbacks(col_index.first, col_index.second, acc_txn, res, defer, rpc_id);
                // to_txn->insert_write_callbacks(acc_txn, res, defer, rpc_id);
            }
        }
        acc_txn->sg.metadata.reads_for_query.clear(); // no need those records anymore, might add more by later dispatch
        acc_txn->sg.metadata.writes_for_query.clear();
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
                    default: verify(0);
                        break;
                }
            }
        }
        for (auto& row_col : acc_txn->sg.metadata.writes_for_query) {
            auto acc_row = dynamic_cast<AccRow *>(row_col.first);
            for (auto &col_index : row_col.second) {
                bool write_safe = acc_row->check_write_status(acc_txn->tid_, col_index.first, col_index.second);
                if (!write_safe) {
                    is_decided = false;
                } else {
                    col_index.second = -1;
                }
            }
        }
    }

    i32 SchedulerAcc::get_row_key(const shared_ptr<SimpleCommand> &sp_piece_data, int32_t var_id, uint8_t workload) const {
        switch (workload) {
            case FACEBOOK:
                // in FB workloads, it's guaranteed that each input_ is a row key
                return sp_piece_data->input.at(var_id).get_i32();
            case SPANNER: {
                if (var_id == SPANNER_TXN_SIZE || var_id == SPANNER_RW_W_COUNT) {
                    //(sp_piece_data->root_type_ == SPANNER_RW);
                    return NOT_ROW_KEY;
                } else {
                    return sp_piece_data->input.at(var_id).get_i32();
                }
            }
            case DYNAMIC: {
                if (var_id == DYNAMIC_TXN_SIZE || var_id == DYNAMIC_RW_W_COUNT) {
                    //verify(sp_piece_data->root_type_ == DYNAMIC_RW);
                    return NOT_ROW_KEY;
                } else {
                    return sp_piece_data->input.at(var_id).get_i32();
                }
            }
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

    void SchedulerAcc::OnResolveStatusCoord(cmdid_t cmd_id, uint8_t *status, DeferredReply* defer) {
        // Log_info("OnResolveStatusCoord. cmd_id = %lu.", cmd_id);
        // Log_info("COORD. OnResolveStatusCoord this svr = %d, receiving cmd_id = %lu.", this->partition_id_, cmd_id);
        txn_status_t decision = resolve_status(cmd_id);
        if (decision != CLEARED) {
            *status = decision;
            defer->reply();
            return;
        }
        // resolve_status needs to query cohorts to reconstruct status, we delay return
        auto tx = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(cmd_id));
        tx->resolve_status_cbs.emplace_back([=](txn_status_t decision) -> void {
            *status = decision;
            defer->reply();
        });
    }

    void SchedulerAcc::AccResolveStatusCoordAck(cmdid_t tid, uint8_t status) {
        // Log_info("AccResolveStatusCoordAck returned tid = %lu; status = %d.", tid, status);
        // Log_info("Cohort. AccResolveStatusCoordAck this svr = %d, receiving cmd_id = %lu, status = %d.", this->partition_id_, tid, status);
        auto tx = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(tid));
        // take action based on status
        if (status != UNCLEARED) {
            verify(status == FINALIZED || status == ABORTED);
            tx->record.status = status;
            OnFinalize(tid, status);
        }
    }

    void SchedulerAcc::OnGetRecord(uint64_t cmd_id, uint8_t *status, uint64_t *ssid_low, uint64_t *ssid_high) {
        // Log_info("Cohort. OnGetRecord. this svr = %d, receiving cmd_id = %lu.", this->partition_id_, cmd_id);
        auto tx = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(cmd_id));
        *status = tx->record.status;
        *ssid_low = tx->record.ts_low;
        *ssid_high = tx->record.ts_high;
    }

    void SchedulerAcc::AccGetRecordAck(uint64_t tid, uint8_t status, uint64_t ssid_low, uint64_t ssid_high) {
        // Log_info("COORD. AccGetRecordAck. this svr = %d, receiving cmd_id = %lu, status = %d, ssid_low = %lu, ssid_high = %lu.",
        //         this->partition_id_, tid, status, ssid_low, ssid_high);
        auto tx = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(tid));
        tx->get_record_acks++;
        tx->returned_records.insert(status);
        if (tx->highest_low < ssid_low) {
            tx->highest_low = ssid_low;
        }
        if (tx->lowest_high > ssid_high) {
            tx->lowest_high = ssid_high;
        }
        if (tx->get_record_acks == tx->get_record_rpcs) {
            // we have received all acks
            txn_status_t decision = CLEARED;
            if (tx->returned_records.find(FINALIZED) != tx->returned_records.end()) {
                decision = FINALIZED;
            } else if (tx->returned_records.find(ABORTED) != tx->returned_records.end()) {
                decision = ABORTED;
            } else if (tx->returned_records.find(UNCLEARED) != tx->returned_records.end()) {
                decision = UNCLEARED;
            }
            tx->returned_records.clear();
            if (decision == CLEARED) {
                // reconstruct client decision
                if (tx->highest_low > tx->lowest_high) {
                    decision = ABORTED; // we omit smart retry for now
                } else {
                    decision = FINALIZED;
                }
            }
            // now we answer to cohorts
            for (auto& callback : tx->resolve_status_cbs) {
                callback(decision);
            }
            tx->resolve_status_cbs.clear();
            // now we take action on coord
            if (decision != UNCLEARED) {
                // decision is either finalized or aborted
                verify(decision == FINALIZED || decision == ABORTED);
                tx->record.status = decision;
                OnFinalize(tid, decision);
            }
        }
    }

    txn_status_t SchedulerAcc::resolve_status(uint64_t cmd_id) {
        auto tx = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(cmd_id));
        if (tx->record.cohorts.empty()) {
            // this is a single-key txn that only has coordinator
            // then it is always consistent
            tx->record.status = FINALIZED;
            OnFinalize(cmd_id, FINALIZED);
        }
        if (tx->record.status != CLEARED || tx->resolving) {
            return tx->record.status;
        }
        // now we need to query cohorts
        tx->resolving = true;
        for (auto cohort : tx->record.cohorts) {
            tx->get_record_rpcs++;
            commo()->AccBroadcastGetRecord(cohort,
                                           cmd_id,
                                           std::bind(&SchedulerAcc::AccGetRecordAck,
                                                     this,
                                                     cmd_id,
                                                     std::placeholders::_1,
                                                     std::placeholders::_2,
                                                     std::placeholders::_3));
        }
        return CLEARED;
    }

    void SchedulerAcc::handle_failure(uint64_t cmd_id, uint32_t pid) {
        Reactor::CreateSpEvent<NeverEvent>()->Wait(FAILURE_TIMEOUT);
        auto txn = GetAccTxn(cmd_id);
        if (txn == nullptr) {
            // this txn was finalized and GC'ed
            return;
        }
        auto tx = dynamic_pointer_cast<AccTxn>(txn);
        // auto tx = dynamic_pointer_cast<AccTxn>(GetOrCreateTx(cmd_id));
        // Log_info("handle_failure triggered. this svr = %d, for cmd_id = %lu.", pid, cmd_id);
        if (tx->record.status != CLEARED) {
            // Log_info("handle_failure triggered early return. this svr = %d, for cmd_id = %lu. current_status = %d. handle_failure = %d.",
            //          pid, cmd_id, tx->record.status, tx->handle_failure);
            return;
        }
        if (tx->record.coord == pid) { // this is coord
            // Log_info("coord. handle_failure. this svr = %d, for cmd_id = %lu.", pid, cmd_id);
            resolve_status(cmd_id);
        } else { // this is cohort
            // Log_info("cohort. handle_failure. this svr = %d, for cmd_id = %lu.", pid, cmd_id);
            auto coord = tx->record.coord;
            commo()->AccBroadcastResolveStatusCoord(coord,
                                                    cmd_id,
                                                    std::bind(&SchedulerAcc::AccResolveStatusCoordAck,
                                                              this,
                                                              cmd_id,
                                                              std::placeholders::_1));
        }
    }

    shared_ptr<Tx> SchedulerAcc::GetAccTxn(uint64_t tid) {
        shared_ptr<Tx> ret = nullptr;
        auto it = dtxns_.find(tid);
        if (it != dtxns_.end()) {
            ret = it->second;
            verify(ret->tid_ == tid);
        }
        return ret;
    }

    i32 SchedulerAcc::get_key(const shared_ptr<SimpleCommand>& c) const {
//                Log_info("Workload type = %d, op count = %d, keys_ size = %d. keys_ real size = %d. keys_ = %d. values size = %d. key = %d.",
//                         c->root_type_, c->input.at(FB_OP_COUNT).get_i32(), c->input.size(), c->input.keys_.size(), *(c->input.keys_.begin()),
//                         c->input.values_->size(), c->input.at(*(c->input.keys_.begin())).get_i32());
//
        int workload = c->root_type_;
        // TODO: we only implement for facebook and spanner for now. TPCC comes later?
        switch(workload) {
            case FB_ROTXN: { //  this is a rotxn in FB, FB input.keys_ only store 1 key per command
                int key_index = *(c->input.keys_.begin());
                return c->input.at(key_index).get_i32();
            }
            case SPANNER_ROTXN: { // this is a rotxn in Spanner, Spanner keys_ store 1 key, a SPANNER_TXN_SIZE, and a SPANNER_RW_W_COUNT
                int key_index = *(c->input.keys_.begin());
                return c->input.at(key_index).get_i32();
            }
            default:
                return -1;
        }
    }
}
