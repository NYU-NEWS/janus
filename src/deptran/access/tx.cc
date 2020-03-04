#include "tx.h"

namespace janus {
    // TODO: fill in these stubs
    bool AccTxn::ReadColumn(Row *row,
                            colid_t col_id,
                            Value* value,
                            int hint_flag) {
        verify(row != nullptr);
        // class downcasting to get AccRow
        auto acc_row = dynamic_cast<AccRow*>(row);
        unsigned long index = 0;
	bool is_decided = true;
        SSID ssid = acc_row->read_column(col_id, value, sg.ssid_spec, sg.offset_safe, index, is_decided);
        row->ref_copy();
	sg.metadata.indices[row][col_id] = index;  // for later validation, could be overwritten by later writes
        if (!is_decided) {
            sg.decided = false;
            sg.metadata.reads_for_query[row][col_id] = index;  // for later AccStatusQuery on read versions
        }
        // update metadata
        sg.update_metadata(ssid.ssid_low, ssid.ssid_high, true);
        return true;
    }

    bool AccTxn::ReadColumns(Row *row,
                             const std::vector<colid_t> &col_ids,
                             std::vector<Value>* values,
                             int hint_flag) {
        verify(row != nullptr);
        auto acc_row = dynamic_cast<AccRow*>(row);
        row->ref_copy();
        for (auto col_id : col_ids) {
            Value *v = nullptr;
            unsigned long index = 0;
	    bool is_decided = true;
	    SSID ssid = acc_row->read_column(col_id, v, sg.ssid_spec, sg.offset_safe, index, is_decided);
            verify(v != nullptr);
            values->push_back(std::move(*v));
            sg.metadata.indices[row][col_id] = index;  // for later validation
	    if (!is_decided) {
                sg.decided = false;
                sg.metadata.reads_for_query[row][col_id] = index;  // for later AccStatusQuery on read versions
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
        auto acc_row = dynamic_cast<AccRow*>(row);
        unsigned long ver_index = 0;
        SSID ssid = acc_row->write_column(col_id, std::move(value), sg.ssid_spec, this->tid_, ver_index, sg.offset_safe); // ver_index is new write
        row->ref_copy();
	    sg.update_metadata(ssid.ssid_low, ssid.ssid_high, false);
        sg.metadata.indices[row][col_id] = ver_index; // for validation and finalize
        return true;
    }

    bool AccTxn::WriteColumns(Row *row,
                              const std::vector<colid_t> &col_ids,
                              std::vector<Value>& values,
                              int hint_flag) {
        verify(row != nullptr);
        auto acc_row = dynamic_cast<AccRow*>(row);
        int v_counter = 0;
        row->ref_copy();
        for (auto col_id : col_ids) {
            unsigned long ver_index = 0;
            SSID ssid = acc_row->write_column(col_id, std::move(values[v_counter++]), sg.ssid_spec, this->tid_, ver_index, sg.offset_safe);
            sg.update_metadata(ssid.ssid_low, ssid.ssid_high, false);
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

    void AccTxn::n_query_inc() {
        sg._n_query++;
    }

    void AccTxn::n_callback_inc() {
        sg._n_query_callback++;
    }

    bool AccTxn::all_callbacks_received() const {
        return sg._n_query == sg._n_query_callback;
    }

    void AccTxn::set_query_abort() {
        sg.status_abort = true;
    }

    int8_t AccTxn::query_result() const {
        return sg.status_abort ? ABORTED : FINALIZED;
    }

    void AccTxn::set_query_done() {
        sg.status_query_done = true;
    }

    bool AccTxn::is_query_done() const {
        return sg.status_query_done;
    }

    void AccTxn::insert_callbacks(shared_ptr<AccTxn> acc_txn, int8_t *res, DeferredReply* defer) {
        query_callbacks.emplace_back([=](int8_t status) -> void {
            if (acc_txn->is_query_done()) { // have done the query by early abort
                return;
            }
            acc_txn->n_callback_inc();
            switch (status) {
                case FINALIZED: // nothing to do
                    break;
                case ABORTED:
                    acc_txn->set_query_abort();
                    break;
                default: verify(0); break;
            }
            if (acc_txn->all_callbacks_received() || acc_txn->query_result() == ABORTED) { // all versions decided or early abort
                // respond to AccStatusQuery RPC
                acc_txn->set_query_done();
                *res = acc_txn->query_result();
                verify(defer != nullptr);
                defer->reply();
            }
        });
    }
}
