#include "tx.h"

namespace janus {
    // TODO: fill in these stubs
    mdb::Row* AccTxn::CreateRow(const mdb::Schema *schema,
                                std::vector<mdb::Value>&& values) {
        verify(values.size() == schema->columns_count());
        auto* new_row = new AccRow();
        return new_row->create(schema, std::move(values));
    }

    bool AccTxn::ReadColumn(Row *row,
                            colid_t col_id,
                            const Value*& value,
                            int hint_flag) {
        verify(row != nullptr);
        // class downcasting to get AccRow
        auto acc_row = dynamic_cast<AccRow*>(row);
        return acc_row->read_column(col_id, value);
    }

    bool AccTxn::ReadColumns(Row *row,
                             const std::vector<colid_t> &col_ids,
                             std::vector<Value>* values,
                             int hint_flag) {
        verify(row != nullptr);
        auto acc_row = dynamic_cast<AccRow*>(row);
        for (auto col_id : col_ids) {
            const Value* v;
            if (acc_row->read_column(col_id, v)) {
                values->push_back(*v);
            } else {
                return false;
            }
        }
        return true;
    }

    bool AccTxn::WriteColumn(Row *row,
                             colid_t col_id,
                             Value&& value,
                             int hint_flag) {
        verify(row != nullptr);
        auto acc_row = dynamic_cast<AccRow*>(row);
        return acc_row->write_column(col_id, std::move(value));
    }

    bool AccTxn::WriteColumns(Row *row,
                              const std::vector<colid_t> &col_ids,
                              std::vector<Value>&& values,
                              int hint_flag) {
        verify(row != nullptr);
        auto acc_row = dynamic_cast<AccRow*>(row);
        for (auto col_id : col_ids) {
            if (!acc_row->write_column(col_id, std::move(values[col_id]))) {
                return false;
            }
        }
        return true;
    }

    /*
    bool AccTxn::InsertRow(Table *tbl, Row *row) {
        return false;
    }
    */

    AccTxn::~AccTxn() {
        // TODO: fill in if holding any resources
        return;
    }
}
