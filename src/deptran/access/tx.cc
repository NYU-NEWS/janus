#include "tx.h"

namespace janus {
    // TODO: fill in these stubs
    mdb::Row* AccTxn::CreateRow(const mdb::Schema *schema,
                                std::vector<mdb::Value>& values) {
        verify(values.size() == schema->columns_count());
        auto* new_row = new AccRow();
        return new_row->create(schema, values);
    }

    bool AccTxn::ReadColumn(Row *row,
                            colid_t col_id,
                            Value *value,
                            int hint_flag) {
        auto acc_row = dynamic_cast<AccRow*>(row);

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
                             Value&& value,
                             int hint_flag) {
        return false;
    }

    bool AccTxn::WriteColumns(Row *row,
                              const std::vector<colid_t> &col_ids,
                              std::vector<Value>&& values,
                              int hint_flag) {
        return false;
    }

    bool AccTxn::InsertRow(Table *tbl, Row *row) {
        return false;
    }

    AccTxn::~AccTxn() {
        return;
    }
}
