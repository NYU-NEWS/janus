#include <memdb/value.h>
#include "row.h"

namespace janus {
    AccRow* AccRow::create(const mdb::Schema *schema,
                           std::vector<mdb::Value>& values) {
        /*
        std::vector<const mdb::Value*> values_ptr(values.size(), nullptr);
        size_t fill_counter = 0;
        for (auto& value : values) {
            fill_values_ptr(schema, values_ptr, value, fill_counter);
            fill_counter++;
        }
        auto* raw_row = new AccRow();
        auto* new_row = (AccRow*)mdb::Row::create(raw_row, schema, values_ptr);
        */
        verify(values.size() == schema->columns_count());
        auto* new_row = new AccRow();
        new_row->schema_ = schema;
        new_row->_row.reserve(schema->columns_count());
        for (int col_id = 0; col_id < values.size(); ++col_id) {
            const mdb::Schema::column_info* info = schema->get_column_info(col_id);
            if (info->indexed) { // this col is a key
                int n_bytes = get_key_type(values.at(col_id));
                char* key = new char[n_bytes];
                values.at(col_id).write_binary(key);
                new_row->keys.emplace_back(key, n_bytes);
            }
            new_row->_row.emplace_back(col_id, std::move(values.at(col_id)));
            /*
            new_row->_row.emplace(
                    std::piecewise_construct,
                    std::make_tuple(col_id),
                    std::make_tuple(col_id, std::move(values.at(col_id))));
            */
        }
        /* Obsolete. because workloads may only populate a subset of cols for each key, e.g., FB
        for (auto col_info_it = schema->begin(); col_info_it != schema->end(); col_info_it++) {
            int col_id = schema->get_column_id(col_info_it->name);
            new_row->_row.emplace(
                    std::piecewise_construct,
                    std::make_tuple(col_id),
                    std::make_tuple(col_id, std::move(values.at(col_id))));
        }
        */
        return new_row;
    }

    SSID AccRow::read_column(mdb::colid_t col_id, mdb::Value* value, snapshotid_t ssid_spec, bool& offset_safe, unsigned long& index, bool& decided) {
        if (col_id >= _row.size()) {
            // col_id is invalid. We're doing a trick here.
            // keys are col_ids
            value = nullptr;
            return {};
        }
        SSID ssid;
        *value = _row.at(col_id).read(ssid_spec, ssid, offset_safe, index, decided);
        return ssid;
    }

    SSID AccRow::write_column(mdb::colid_t col_id, mdb::Value&& value, snapshotid_t ssid_spec, txnid_t tid, unsigned long& ver_index, bool& offset_safe, bool& abort) {
        if (col_id >= _row.size()) {
            return {};
        }
        // push back to the txn queue
        return _row.at(col_id).write(std::move(value), ssid_spec, tid, ver_index, offset_safe, abort);
    }

    bool AccRow::validate(txnid_t tid, mdb::colid_t col_id, unsigned long index, snapshotid_t ssid_new, bool validate_consistent) {
	    if (_row[col_id].is_read(tid, index)) {
            // this is validating a read
            if (!validate_consistent || (!_row[col_id].is_logical_head(index) && _row[col_id].next_record_ssid(index) <= ssid_new)) {
                // validate fails if there is new logical head && we cannot extend ssid to new
                return false;
            }
            _row[col_id].txn_queue[index].extend_ssid(ssid_new);  // extend ssid range for reads
            return true;
        } else {
            // validating a write
            if (_row[col_id].txn_queue[index].ssid.ssid_low == ssid_new) {
                // this write is the new snapshot
                _row[col_id].txn_queue[index].status = VALIDATING;
                return true;
            }
            if (!validate_consistent || (!_row[col_id].is_logical_head(index) && _row[col_id].next_record_ssid(index) <= ssid_new)
                || _row[col_id].txn_queue[index].have_reads()) {
                // validation fails. TODO: can we optimize the have_reads() case?
                _row[col_id].txn_queue[index].status = ABORTED;  // will abort anyways
                return false;
            }
            _row[col_id].txn_queue[index].update_ssid(ssid_new);  // update both low and high to new for writes
            _row[col_id].txn_queue[index].status = VALIDATING;
            return true;
        }
    }

    void AccRow::finalize(txnid_t tid, mdb::colid_t col_id, unsigned long ver_index, int8_t decision) {
        if (_row[col_id].is_read(tid, ver_index)) {
            // deciding a read, nothing to do
            // TODO: should not send rpcs for deciding reads in the first place
            return;
        }
        //Log_info("tid: %lu; finaling a record. decision = %d; index = %lu", tid, decision, ver_index);
        _row[col_id].finalize(ver_index, decision);
    }

    int8_t AccRow::check_status(mdb::colid_t col_id, unsigned long index) {
        return _row[col_id].txn_queue[index].status;
    }

    txnid_t AccRow::get_ver_tid(mdb::colid_t col_id, unsigned long index) {
        return _row[col_id].txn_queue[index].txn_id;
    }

    int AccRow::get_key_type(const mdb::Value& value) {
        switch (value.get_kind()) {
            case mdb::Value::I32:
                return sizeof(i32);
            case mdb::Value::I64:
                return sizeof(i64);
            case mdb::Value::DOUBLE:
                return sizeof(double);
            case mdb::Value::STR:
                return value.get_str().length();
            default: verify(0);
                break;
        }
    }

    mdb::MultiBlob AccRow::get_key() const {
        //const std::vector<int>& key_cols = schema_->key_columns_id();
        mdb::MultiBlob mb(keys.size());
        for (int i = 0; i < mb.count(); i++) {
            mdb::blob b;
            b.data = keys.at(i).first;
            b.len = keys.at(i).second;
            mb[i] = b;
        }
        return mb;
    }

    mdb::Value AccRow::get_column(int column_id) const {
        //verify(_row.find(column_id) != _row.end());
        verify(_row.size() > column_id);
        verify(!_row.at(column_id).txn_queue.empty());
        mdb::Value v = _row.at(column_id).txn_queue.at(0).value;
        return v;
    }

    mdb::blob AccRow::get_blob(int column_id) const {
        //verify(_row.find(column_id) != _row.end());
        verify(_row.size() > column_id);
        verify(!_row.at(column_id).txn_queue.empty());
        mdb::blob b;
        Value v = _row.at(column_id).txn_queue.at(0).value;
        int n_bytes = get_key_type(v);
        char* v_data = new char[n_bytes];
        v.write_binary(v_data);
        b.data = v_data;
        b.len = n_bytes;
        return b;
    }

    AccRow::~AccRow() {
        for (auto& key : keys) {
            delete[] key.first;
        }
        _row.clear();
    }
}
