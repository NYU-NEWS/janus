#ifndef DEP_ROW_H_
#define DEP_ROW_H_

#include "../__dep__.h"

namespace janus {

struct entry_t;

class RccRow : public mdb::VersionedRow {
private:

protected:

    // protected dtor as required by RefCounted
    ~RccRow() override;


public:

    entry_t *dep_entry_{nullptr};

    void copy_into(RccRow * row) const;

    void init_dep(int n_columns);

    mdb::Row* copy() const override {
        RccRow * row = new RccRow();
        copy_into(row);
        return row;
    }

    virtual entry_t *get_dep_entry(int col_id);

    template <class Container>
    static RccRow * create(const mdb::Schema* schema, const Container& values) {
        verify(values.size() == schema->columns_count());
        std::vector<const Value*> values_ptr(values.size(), nullptr);
        size_t fill_counter = 0;
        for (auto it = values.begin(); it != values.end(); ++it) {
            fill_values_ptr(schema, values_ptr, *it, fill_counter);
            fill_counter++;
        }
        RccRow * raw_row = new RccRow();
        raw_row->init_dep(schema->columns_count());
        raw_row->init_ver(schema->columns_count());
        return (RccRow * ) mdb::Row::create(raw_row, schema, values_ptr);
    }
};

} // namespace janus

#endif // DEP_ROW_H_
