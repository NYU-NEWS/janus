#include "tx.h"

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
        if (sg.abort) {
            get_dummy_value(value);
            return true;
        }
        verify(row != nullptr);
        // class downcasting to get AccRow
        auto acc_row = dynamic_cast<AccRow*>(row);
        unsigned long index = 0;
	    bool is_decided = true;
        //Log_info("server:ReadColumn. txid = %lu. ssid_spec = %lu.", this->tid_, sg.ssid_spec);
        SSID ssid = acc_row->read_column(col_id, value, sg.ssid_spec, sg.offset_safe, index, is_decided, sg.abort);
        if (!sg.abort) {
            row->ref_copy();
            sg.metadata.indices[row][col_id] = index;  // for later validation, could be overwritten by later writes
            if (!is_decided) {
                sg.decided = false;
                row->ref_copy();
                sg.metadata.reads_for_query[row][col_id] = index;  // for later AccStatusQuery on read versions
            }
            // update metadata
            sg.update_metadata(ssid.ssid_low, ssid.ssid_high, true);
        }
        return true;
    }

    bool AccTxn::ReadColumns(Row *row,
                             const std::vector<colid_t> &col_ids,
                             std::vector<Value>* values,
                             int hint_flag) {
        verify(row != nullptr);
        auto acc_row = dynamic_cast<AccRow*>(row);
        for (auto col_id : col_ids) {
            Value v;
            if (sg.abort) {
                get_dummy_value(&v);
                values->push_back(v);
                continue;
            }
            unsigned long index = 0;
	        bool is_decided = true;
	        SSID ssid = acc_row->read_column(col_id, &v, sg.ssid_spec, sg.offset_safe, index, is_decided, sg.abort);
            if (!sg.abort) {
                values->push_back(std::move(v));
                row->ref_copy();
                sg.metadata.indices[row][col_id] = index;  // for later validation
                if (!is_decided) {
                    row->ref_copy();
                    sg.decided = false;
                    sg.metadata.reads_for_query[row][col_id] = index;  // for later AccStatusQuery on read versions
                }
                // update metadata
                sg.update_metadata(ssid.ssid_low, ssid.ssid_high, true);
            }
        }
        return true;
    }

    bool AccTxn::WriteColumn(Row *row,
                             colid_t col_id,
                             Value& value,
                             int hint_flag) {
        if (sg.abort) {
            return true;
        }
        verify(row != nullptr);
        auto acc_row = dynamic_cast<AccRow*>(row);
        unsigned long ver_index = 0;
        //Log_info("server:WriteColumn. txid = %lu. ssid_spec = %lu.", this->tid_, sg.ssid_spec);
        SSID ssid = acc_row->write_column(col_id, std::move(value), sg.ssid_spec, this->tid_, ver_index, sg.offset_safe, sg.abort); // ver_index is new write
        if (!sg.abort) {
            row->ref_copy();
            sg.update_metadata(ssid.ssid_low, ssid.ssid_high, false);
            sg.metadata.indices[row][col_id] = ver_index; // for validation and finalize
        }
        return true;
    }

    bool AccTxn::WriteColumns(Row *row,
                              const std::vector<colid_t> &col_ids,
                              std::vector<Value>& values,
                              int hint_flag) {
        if (sg.abort) {
            return true;
        }
        verify(row != nullptr);
        auto acc_row = dynamic_cast<AccRow*>(row);
        int v_counter = 0;
        for (auto col_id : col_ids) {
            unsigned long ver_index = 0;
            SSID ssid = acc_row->write_column(col_id, std::move(values[v_counter++]), sg.ssid_spec, this->tid_, ver_index, sg.offset_safe, sg.abort);
            if (!sg.abort) {
                sg.update_metadata(ssid.ssid_low, ssid.ssid_high, false);
                row->ref_copy();
                sg.metadata.indices[row][col_id] = ver_index; // for validation and finalize
            }
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
}
