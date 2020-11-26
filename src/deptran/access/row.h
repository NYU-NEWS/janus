//
// Created by chance_Lv on 2020/2/5.
//
#pragma once

//#include "linked_vector.h"

#include "column.h"
#include "tx.h"
#include <unordered_map>
#include <memdb/row.h>
/*
 * Defines the row structure in ACCESS.
 * A row is a map of txn_queue (linked_vector); a txn_queue for each col
 */
namespace janus {
    class AccTxn;
    class AccRow : public mdb::Row {
    public:
        static AccRow* create(const mdb::Schema *schema, std::vector<mdb::Value>& values);
        SSID read_column(txnid_t tid, mdb::colid_t col_id, mdb::Value* value, snapshotid_t ssid_spec, bool& offset_safe, unsigned long& index, bool& decided, bool& abort);
        SSID write_column(mdb::colid_t col_id, mdb::Value&& value, snapshotid_t ssid_spec, txnid_t tid, unsigned long& ver_index,
                bool& offset_safe, bool& abort, bool disable_early_abort = false, bool mark_finalized = false);
        bool validate(txnid_t tid, mdb::colid_t col_id, unsigned long index, snapshotid_t ssid_new, bool validate_consistent);
        void finalize(txnid_t tid, mdb::colid_t col_id, unsigned long ver_index, int8_t decision);
        int8_t check_status(mdb::colid_t col_id, unsigned long index);
	    txnid_t get_ver_tid(mdb::colid_t col_id, unsigned long index);
        mdb::MultiBlob get_key() const override;
        mdb::Value get_column(int column_id) const override;  // used by tpcc population
        mdb::blob get_blob(int column_id) const override;
	    ~AccRow() override;
    private:
        // a map of txn_q; keys are cols, values are linkedvectors that holding txns (versions)
        // std::unordered_map<mdb::colid_t, AccColumn> _row;
        std::vector<AccColumn> _row;
        std::vector<std::pair<char*, int>> keys;
        static int get_key_type(const mdb::Value& value);
    };
}   // namespace janus
