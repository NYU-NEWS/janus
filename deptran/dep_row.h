#ifndef DEP_ROW_H_
#define DEP_ROW_H_

#include "memdb/row.h"
#include <vector>

namespace rococo {

struct entry_t;

class DepRow : public mdb::CoarseLockedRow {
private:
    entry_t *dep_entry_;
    void init_dep(int n_columns);

protected:

    // protected dtor as required by RefCounted
    ~DepRow();

    void copy_into(DepRow* row) const;

public:

    virtual mdb::Row* copy() const {
        DepRow* row = new DepRow();
        copy_into(row);
        return row;
    }

    virtual entry_t *get_dep_entry(int col_id);

    template <class Container>
    static DepRow* create(const mdb::Schema* schema, const Container& values) {
        verify(values.size() == schema->columns_count());
        std::vector<const Value*> values_ptr(values.size(), nullptr);
        size_t fill_counter = 0;
        for (auto it = values.begin(); it != values.end(); ++it) {
            fill_values_ptr(schema, values_ptr, *it, fill_counter);
            fill_counter++;
        }
        DepRow* raw_row = new DepRow();
        raw_row->init_dep(schema->columns_count());
        return (DepRow * ) mdb::Row::create(raw_row, schema, values_ptr);
    }
};

} // namespace rcc

#endif // DEP_ROW_H_
