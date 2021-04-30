#include "tx.h"
#include "ssid_predictor.h"

namespace janus {
    const mdb::Value AccTxn::DUMMY_VALUE_STR{""};
    const mdb::Value AccTxn::DUMMY_VALUE_I32{(i32)0};
    const mdb::Value AccTxn::DUMMY_VALUE_I64{(i64)0};
    const mdb::Value AccTxn::DUMMY_VALUE_DOUBLE{(double)0};

    // TODO: fill in these stubs
    bool AccTxn::ReadColumn(Row *row,
                            colid_t col_id,
                            Value* value,
                            int hint_flag) {
        verify(row != nullptr);

        if (this->early_abort) {
            get_dummy_value(value);
            return true;
        }

        // class downcasting to get AccRow
        auto acc_row = dynamic_cast<AccRow*>(row);
        unsigned long index = 0;
	    bool is_decided = true;
	    bool rotxn_safe = true;
	    bool abort = false;
	    // Log_info("ReadColumn. txnid = %lu. key = %d. ts = %d.", this->tid_, acc_row->key, this->key_to_ts[acc_row->key]);
        //Log_info("server:ReadColumn. txid = %lu. ssid_spec = %lu.", this->tid_, sg.ssid_spec);
        SSID ssid = acc_row->read_column(this->tid_, col_id, value, sg.ssid_spec, index, is_decided, this->is_rotxn, rotxn_safe, this->safe_ts, abort);
        // for rotxn
        // i32 key = acc_row->key;
        // uint64_t new_ts = ssid.ssid_low;
        // update_return_ts(key, new_ts);
        /*
        if (this->safe_ts < acc_row->write_ts) {
            this->rotxn_okay = false;
        }
        */
        if (abort) {
            this->early_abort = true;
            return true;
        }
        if (this->is_rotxn && !rotxn_safe) {
            this->rotxn_okay = false;
        }

        row->ref_copy();
        sg.metadata.indices[row][col_id] = index;  // for later validation, could be overwritten by later writes
        if (!is_decided) {
            sg.decided = false;
            row->ref_copy();
            sg.metadata.reads_for_query[row][col_id] = index;  // for later AccStatusQuery on read versions
            // Log_info("txnid = %lu; read waiting on tx = %lu. col = %d, index = %d.",
            //         this->tid_, acc_row->_row[col_id].txn_queue[index].txn_id, col_id, index);
        } else {
            //Log_info("txnid = %lu; decided read on tx = %lu. col = %d, index = %d.",
            //         this->tid_, acc_row->_row[col_id].txn_queue[index].txn_id, col_id, index);
        }
        // update metadata
        sg.update_metadata(ssid.ssid_low, ssid.ssid_high, true);
        // Log_info("after read. n_reads = %d", acc_row->_row[col_id].txn_queue[index].n_pending_reads);
        return true;
    }

    bool AccTxn::ReadColumns(Row *row,
                             const std::vector<colid_t> &col_ids,
                             std::vector<Value>* values,
                             int hint_flag) {
        verify(row != nullptr);
        auto acc_row = dynamic_cast<AccRow*>(row);
        i32 key = acc_row->key;
        for (auto col_id : col_ids) {
            Value *v = nullptr;
            if (this->early_abort) {
                get_dummy_value(v);
                values->push_back(*v);
                continue;
            }

            unsigned long index = 0;
	        bool is_decided = true;
            bool rotxn_safe = true;
            bool abort = false;
            SSID ssid = acc_row->read_column(this->tid_, col_id, v, sg.ssid_spec, index, is_decided, this->is_rotxn, rotxn_safe, this->safe_ts, abort);
            // for rotxn
            // uint64_t new_ts = ssid.ssid_low;
            // update_return_ts(key, new_ts);
            /*
            if (new_ts != this->key_to_ts[key]) {
                this->rotxn_okay = false;
            }
            */
            /*
            if (this->safe_ts < acc_row->write_ts) {
                this->rotxn_okay = false;
            }
            */
            if (this->is_rotxn && !rotxn_safe) {
                this->rotxn_okay = false;
            }

            verify(v != nullptr);
            values->push_back(std::move(*v));
            if (abort) {
                this->early_abort = true;
                continue;
            }
            row->ref_copy();
            sg.metadata.indices[row][col_id] = index;  // for later validation
            if (!is_decided) {
                row->ref_copy();
                sg.decided = false;
                sg.metadata.reads_for_query[row][col_id] = index;  // for later AccStatusQuery on read versions
//                Log_info("txnid = %lu; read waiting on tx = %lu. col = %d, index = %d.",
//                         this->tid_, acc_row->_row[col_id].txn_queue[index].txn_id, col_id, index);
            }
            // update metadata
            sg.update_metadata(ssid.ssid_low, ssid.ssid_high, true);
        }
        return true;
    }

    bool AccTxn::WriteColumn(Row *row,
                             colid_t col_id,
                             Value& value,
                             int hint_flag) {
        verify(row != nullptr);

        if (this->early_abort) {
            return true;
        }

        auto acc_row = dynamic_cast<AccRow*>(row);
        unsigned long ver_index = 0;
        //Log_info("server:WriteColumn. txid = %lu. ssid_spec = %lu.", this->tid_, sg.ssid_spec);
        bool is_decided = true;
        unsigned long prev_index = 0;
        bool same_tx = false;
        bool abort = false;
        SSID ssid = acc_row->write_column(col_id, std::move(value), sg.ssid_spec, this->tid_, ver_index,
                is_decided, prev_index, same_tx, abort,sg.mark_finalized); // ver_index is new write
        // for rotxn
        /*
        i32 key = acc_row->key;
        uint64_t new_ts = ssid.ssid_low;
        update_return_ts(key, new_ts);
        */
        uint64_t current_time = SSIDPredictor::get_current_time();
        if (acc_row->write_ts < current_time) {
            acc_row->write_ts = current_time;
        }
        if (this->write_ts < acc_row->write_ts) {
            this->write_ts = acc_row->write_ts;
        }
        if (abort) {
            this->early_abort = true;
            return true;
        }

        if (same_tx) {
            return true;
        }
        row->ref_copy();
        sg.update_metadata(ssid.ssid_low, ssid.ssid_high, false);
        // for ss
        /*
        if (sg.metadata.indices[row].find(col_id) != sg.metadata.indices[row].end()) {
            // the same tx has a read on the same col.
            unsigned long pre_index = sg.metadata.indices[row].at(col_id);
            //Log_info("txnid = %lu; write: prev_read colid = %d; index = %d.", this->tid_, col_id, pre_index);
            verify(acc_row->_row.at(col_id).txn_queue[pre_index].pending_reads.find(this->tid_) != acc_row->_row.at(col_id).txn_queue[pre_index].pending_reads.end());
            // verify(!acc_row->_row.at(col_id).txn_queue[pre_index].pending_reads.empty());
    // verify(pre_index == prev_index);
            // acc_row->_row.at(col_id).txn_queue[pre_index].n_pending_reads--;
            acc_row->_row.at(col_id).txn_queue[pre_index].pending_reads.erase(this->tid_);
            if (acc_row->check_write_status(this->tid_, col_id, pre_index)) {
                is_decided = true;
            }
        }
        if (!is_decided) {
            sg.decided = false;
            row->ref_copy();
            // Log_info("txnid = %lu; write waiting on tx = %lu. col = %d, index = %d.",
            //         this->tid_, acc_row->_row[col_id].txn_queue[prev_index].txn_id, col_id, prev_index);
            sg.metadata.writes_for_query[row][col_id] = prev_index;  // for later AccStatusQuery on write versions
        } else {
            //Log_info("txnid = %lu; decided write on tx = %lu. col = %d, index = %d.",
            //         this->tid_, acc_row->_row[col_id].txn_queue[prev_index].txn_id, col_id, prev_index);
        }
        */
        sg.metadata.indices[row][col_id] = ver_index; // for validation and finalize
        return true;
    }

    bool AccTxn::WriteColumns(Row *row,
                              const std::vector<colid_t> &col_ids,
                              std::vector<Value>& values,
                              int hint_flag) {
        verify(row != nullptr);
        auto acc_row = dynamic_cast<AccRow*>(row);
        i32 key = acc_row->key;
        int v_counter = 0;
        for (auto col_id : col_ids) {
            if (this->early_abort) {
                return true;
            }
            unsigned long ver_index = 0;
            bool is_decided = true;
            unsigned long prev_index = 0;
            bool same_tx = false;
            bool abort = false;
            SSID ssid = acc_row->write_column(col_id, std::move(values[v_counter++]), sg.ssid_spec, this->tid_,
                    ver_index, is_decided, prev_index, same_tx, abort,sg.mark_finalized);
            // for rotxn
            // uint64_t new_ts = ssid.ssid_low;
            // update_return_ts(key, new_ts);
            uint64_t current_time = SSIDPredictor::get_current_time();
            if (acc_row->write_ts < current_time) {
                acc_row->write_ts = current_time;
            }
            if (this->write_ts < acc_row->write_ts) {
                this->write_ts = acc_row->write_ts;
            }
            if (abort) {
                this->early_abort = true;
                return true;
            }
            if (same_tx) {
                continue;
            }
            sg.update_metadata(ssid.ssid_low, ssid.ssid_high, false);
            row->ref_copy();
            // for ss
            /*
            if (sg.metadata.indices[row].find(col_id) != sg.metadata.indices[row].end()) {
                // the same tx has a read on the same col.
                unsigned long pre_index = sg.metadata.indices[row].at(col_id);
                // verify(acc_row->_row.at(col_id).txn_queue[pre_index].n_pending_reads > 0);
                verify(acc_row->_row.at(col_id).txn_queue[pre_index].pending_reads.find(this->tid_) != acc_row->_row.at(col_id).txn_queue[pre_index].pending_reads.end());
        // verify(pre_index == prev_index);
                // acc_row->_row.at(col_id).txn_queue[pre_index].n_pending_reads--;
                acc_row->_row.at(col_id).txn_queue[pre_index].pending_reads.erase(this->tid_);
                if (acc_row->check_write_status(this->tid_, col_id, pre_index)) {
                    is_decided = true;
                }
            }
            if (!is_decided) {
                sg.decided = false;
                row->ref_copy();
                sg.metadata.writes_for_query[row][col_id] = prev_index;  // for later AccStatusQuery on write versions
                // Log_info("txnid = %lu; write waiting on tx = %lu. col = %d, index = %d.",
                //          this->tid_, acc_row->_row[col_id].txn_queue[prev_index].txn_id, col_id, prev_index);
            }
            */
            sg.metadata.indices[row][col_id] = ver_index; // for validation and finalize
        }
        return true;
    }

    AccTxn::~AccTxn() {
        // TODO: fill in if holding any resources
    }

    void AccTxn::load_speculative_ssid(snapshotid_t ssid) {
        sg.ssid_spec = ssid;
    }

    snapshotid_t AccTxn::get_spec_ssid() const {
        return sg.ssid_spec;
    }

    int AccTxn::n_query_inc() {
	    return ++sg._n_query;
    }

    int AccTxn::n_callback_inc() {
	    return ++sg._n_query_callback;
    }

    bool AccTxn::all_callbacks_received() const {
        return sg._n_query == sg._n_query_callback;
    }

    void AccTxn::set_query_abort() {
        sg.status_abort = true;
    }

    bool AccTxn::is_status_abort() const {
        return sg.status_abort;
    }

    bool AccTxn::is_query_done() const {
        return sg.status_query_done;
    }

    int8_t AccTxn::query_result() const {
        return sg.status_abort ? ABORTED : FINALIZED;
    }

    void AccTxn::insert_callbacks(const shared_ptr<AccTxn>& acc_txn, int8_t *res, DeferredReply* defer, int rpc_id) {
        query_callbacks.emplace_back([=](int8_t status) -> void {
	        if (acc_txn->subrpc_status[rpc_id] == ABORTED) { // have done the query by early abort
                acc_txn->record.status = ABORTED;
                return;
            }
	        acc_txn->subrpc_count[rpc_id]--;
            switch (status) {
                case FINALIZED: // nothing to do
                    break;
                case ABORTED:
                    acc_txn->set_query_abort();
		            acc_txn->subrpc_status[rpc_id] = ABORTED;
                    break;
                default: verify(0); break;
            }
	        if (acc_txn->subrpc_count[rpc_id] == 0 || acc_txn->subrpc_status[rpc_id] == ABORTED) { // all versions decided or early abort
                // respond to AccStatusQuery RPC
                *res = acc_txn->query_result();
                verify(defer != nullptr);
                // failure handling, this txn's querystate rpc has responded
                if (acc_txn->record.status == UNCLEARED) {
                    acc_txn->record.status = CLEARED;
                }
                if (*res == ABORTED) {
                    acc_txn->record.status = ABORTED;
                }

                defer->reply();
            }
        });
    }

    void AccTxn::get_dummy_value(mdb::Value *value) {
        verify(value != nullptr);
        switch (value->get_kind()) {
            case mdb::Value::STR:
                *value = DUMMY_VALUE_STR;
                break;
            case mdb::Value::I64:
                *value = DUMMY_VALUE_I64;
                break;
            case mdb::Value::DOUBLE:
                *value = DUMMY_VALUE_DOUBLE;
                break;
            case mdb::Value::UNKNOWN:
            case mdb::Value::I32:
                *value = DUMMY_VALUE_I32;
                break;
            default: verify(0);
                break;
        }
    }

    /*
    void AccTxn::update_return_ts(i32 key, uint64_t new_ts) {
        // if (return_key_ts.find(key) == return_key_ts.end() || return_key_ts.at(key) < new_ts) {
        // new_ts could be smaller if the write was aborted
            return_key_ts[key] = new_ts;
        // }
    }
    */
}
