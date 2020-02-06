#include "tx.h"

namespace janus {
    // TODO: fill in these stubs
    AccRow* AccTxn::CreateRow(const mdb::Schema *schema,
                              const std::vector<mdb::Value> &values) {
        return nullptr;
    }

    bool AccTxn::ReadColumn(Row *row,
                            colid_t col_id,
                            Value *value,
                            int hint_flag) {
        return false;
    }

    bool AccTxn::ReadColumns(Row *row,
                             const std::vector<colid_t> &col_ids,
                             std::vector<Value> *values,
                             int hint_flag) {
        return false;
    }

    bool AccTxn::WriteColumn(Row *row,
                             colid_t col_id,
                             const Value &value,
                             int hint_flag) {
        return false;
    }

    bool AccTxn::WriteColumns(Row *row,
                              const std::vector<colid_t> &col_ids,
                              const std::vector<Value> &values,
                              int hint_flag) {
        return false;
    }

    bool AccTxn::InsertRow(Table *tbl, Row *row) {
        return false;
    }

    AccRow* AccTxn::Query(mdb::Table *tbl,
                          const mdb::MultiBlob &mb,
                          int64_t row_context_id) {
        return nullptr;
    }

    AccRow* AccTxn::Query(mdb::Table *tbl,
                          vector<Value> &primary_keys,
                          int64_t row_context_id) {
        return nullptr;
    }

    AccTxn::~AccTxn() {
        return;
    }
}
