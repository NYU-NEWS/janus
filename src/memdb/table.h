#pragma once

#include <string>
#include <list>
#include <unordered_map>
#include <boost/crc.hpp>      // for boost::crc_basic, boost::crc_optimal

#include "value.h"
#include "row.h"
#include "schema.h"
#include "utils.h"
#include "blob.h"

#include "snapshot.h"


namespace mdb {

// Tables are NoCopy, because they might maintain a pointer to schema, which should not be shared
class Table: public NoCopy {
protected:
    std::string name_;
    const Schema* schema_;

public:
    Table(std::string name, const Schema* schema): name_(name), schema_(schema) {
        // prevent further changes
        const_cast<Schema*>(schema_)->freeze();
    }
    virtual ~Table() {}

    const Schema* schema() const { return schema_; }
    const std::string Name() const { return name_; }

    virtual uint16_t Checksum() {
      verify(0);
      return 0;
    }
    virtual void insert(Row* row) = 0;
    virtual void remove(Row* row, bool do_free = true) = 0;
    virtual uint64_t size() {verify(0); return 0;}
    virtual void notify_before_update(Row* row, int updated_column_id) {
        // used to notify IndexedTable to update secondary index
    }

    virtual void notify_after_update(Row* row, int updated_column_id) {
        // used to notify IndexedTable to update secondary index
    }

    virtual symbol_t rtti() const = 0;
};

class SortedMultiKey {
    MultiBlob mb_;
    const Schema* schema_;
public:
    SortedMultiKey(const MultiBlob& mb, const Schema* schema): mb_(mb), schema_(schema) {
        verify(mb_.count() == (int) schema->key_columns_id().size());
    }

    // -1: this < o, 0: this == o, 1: this > o
    // UNKNOWN == UNKNOWN
    // both side should have same kind
    int compare(const SortedMultiKey& o) const;

    bool operator ==(const SortedMultiKey& o) const {
        return compare(o) == 0;
    }
    bool operator !=(const SortedMultiKey& o) const {
        return compare(o) != 0;
    }
    bool operator <(const SortedMultiKey& o) const {
        return compare(o) == -1;
    }
    bool operator >(const SortedMultiKey& o) const {
        return compare(o) == 1;
    }
    bool operator <=(const SortedMultiKey& o) const {
        return compare(o) != 1;
    }
    bool operator >=(const SortedMultiKey& o) const {
        return compare(o) != -1;
    }

    const MultiBlob& get_multi_blob() const {
        return mb_;
    }
};

class SortedTable: public Table {
protected:
    typedef std::multimap<SortedMultiKey, Row*>::const_iterator iterator;
    typedef std::multimap<SortedMultiKey, Row*>::const_reverse_iterator reverse_iterator;

    virtual iterator remove(iterator it, bool do_free = true);

    // indexed by key values
    std::multimap<SortedMultiKey, Row*> rows_;
public:

    class Cursor: public Enumerator<const Row*> {
        iterator begin_, end_, next_;
        reverse_iterator r_begin_, r_end_, r_next_;
        int count_;
        bool reverse_;
    public:
        Cursor(const iterator& begin, const iterator& end): count_(-1), reverse_(false) {
            begin_ = begin;
            end_ = end;
            next_ = begin;
        }
        Cursor(const reverse_iterator& begin, const reverse_iterator& end): count_(-1), reverse_(true) {
            r_begin_ = begin;
            r_end_ = end;
            r_next_ = begin;
        }

        void reset() {
            if (reverse_) {
                r_next_ = r_begin_;
            } else {
                next_ = begin_;
            }
        }

        const iterator& begin() const {
            return begin_;
        }
        const iterator& end() const {
            return end_;
        }
        const reverse_iterator& rbegin() const {
            return r_begin_;
        }
        const reverse_iterator& rend() const {
            return r_end_;
        }
        bool has_next() {
            if (reverse_) {
                return r_next_ != r_end_;
            } else {
                return next_ != end_;
            }
        }
        operator bool () {
            return has_next();
        }
        Row* next() {
            Row* row = nullptr;
            if (reverse_) {
                verify(r_next_ != r_end_);
                row = r_next_->second;
                ++r_next_;
            } else {
                verify(next_ != end_);
                row = next_->second;
                ++next_;
            }
            return row;
        }
        int count() {
            if (count_ < 0) {
                count_ = 0;
                if (reverse_) {
                    for (auto it = r_begin_; it != r_end_; ++it) {
                        count_++;
                    }
                } else {
                    for (auto it = begin_; it != end_; ++it) {
                        count_++;
                    }
                }
            }
            return count_;
        }
    };

    virtual uint64_t size() override {return rows_.size();}

    SortedTable(std::string name, const Schema* schema): Table(name, schema) {}

    ~SortedTable();

    virtual symbol_t rtti() const override {
        return TBL_SORTED;
    }

    virtual uint16_t Checksum() override {
      auto v = std::make_unique<vector<uint16_t>>(rows_.size());
      auto i = 0;
      uint16_t ret = 0;
      for (auto pair: rows_) {
        auto& row = pair.second;
        auto c = row->Checksum();
        i++;
        ret ^= c;
      }
//      boost::crc_basic<16>  crc_ccitt1( 0x1021, 0xFFFF, 0, false, false );
//      crc_ccitt1.process_bytes( v->data(), v->size() * sizeof((*v)[0]));
//      auto r = crc_ccitt1.checksum();
      return ret;
    }

    void insert(Row* row) override {
        SortedMultiKey key = SortedMultiKey(row->get_key(), schema_);
        verify(row->schema() == schema_);
        row->set_table(this);
        insert_into_map(rows_, key, row);
    }

    Cursor query(const Value& kv) {
        return query(kv.get_blob());
    }
    Cursor query(const MultiBlob& mb) {
        return query(SortedMultiKey(mb, schema_));
    }
    Cursor query(const SortedMultiKey& smk) {
        // auto first = rows_.begin();
        // auto last = rows_.rbegin();
        auto range = rows_.equal_range(smk);
        return Cursor(range.first, range.second);
    }

    Cursor query_lt(const Value& kv, symbol_t order = symbol_t::ORD_ASC) {
        return query_lt(kv.get_blob(), order);
    }
    Cursor query_lt(const MultiBlob& mb, symbol_t order = symbol_t::ORD_ASC) {
        return query_lt(SortedMultiKey(mb, schema_), order);
    }
    Cursor query_lt(const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC) {
        verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC || order == symbol_t::ORD_ANY);
        auto bound = rows_.lower_bound(smk);
        if (order == symbol_t::ORD_DESC) {
            return Cursor(reverse_iterator(bound), rows_.rend());
        } else {
            return Cursor(rows_.begin(), bound);
        }
    }

    Cursor query_gt(const Value& kv, symbol_t order = symbol_t::ORD_ASC) {
        return query_gt(kv.get_blob(), order);
    }
    Cursor query_gt(const MultiBlob& mb, symbol_t order = symbol_t::ORD_ASC) {
        return query_gt(SortedMultiKey(mb, schema_), order);
    }
    Cursor query_gt(const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC) {
        verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC || order == symbol_t::ORD_ANY);
        auto bound = rows_.upper_bound(smk);
        if (order == symbol_t::ORD_DESC) {
            return Cursor(rows_.rbegin(), reverse_iterator(bound));
        } else {
            return Cursor(bound, rows_.end());
        }
    }

    // (low, high) not inclusive
    Cursor query_in(const Value& low, const Value& high, symbol_t order = symbol_t::ORD_ASC) {
        return query_in(low.get_blob(), high.get_blob(), order);
    }
    Cursor query_in(const MultiBlob& low, const MultiBlob& high, symbol_t order = symbol_t::ORD_ASC) {
        return query_in(SortedMultiKey(low, schema_), SortedMultiKey(high, schema_), order);
    }
    Cursor query_in(const SortedMultiKey& low, const SortedMultiKey& high, symbol_t order = symbol_t::ORD_ASC) {
        verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC || order == symbol_t::ORD_ANY);
        verify(low < high);
        auto low_bound = rows_.upper_bound(low);
        auto high_bound = rows_.lower_bound(high);
        if (order == symbol_t::ORD_DESC) {
            return Cursor(reverse_iterator(high_bound), reverse_iterator(low_bound));
        } else {
            return Cursor(low_bound, high_bound);
        }
    }

    Cursor all(symbol_t order = symbol_t::ORD_ASC) const {
        verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC || order == symbol_t::ORD_ANY);
        if (order == symbol_t::ORD_DESC) {
            return Cursor(rows_.rbegin(), rows_.rend());
        } else {
            return Cursor(std::begin(rows_), std::end(rows_));
        }
    }

    void clear();

    void remove(const Value& kv) {
        remove(kv.get_blob());
    }
    void remove(const MultiBlob& mb) {
        remove(SortedMultiKey(mb, schema_));
    }
    void remove(const SortedMultiKey& smk);
    void remove(Row* row, bool do_free = true) override;
    void remove(Cursor cur);
};


class UnsortedTable: public Table {
    typedef std::unordered_multimap<MultiBlob, Row*, MultiBlob::hash>::const_iterator iterator;

public:

    class Cursor: public Enumerator<const Row*> {
        iterator begin_, end_, next_;
        int count_;
    public:
        Cursor(const iterator& begin, const iterator& end): begin_(begin), end_(end), next_(begin), count_(-1) {}

        void reset() {
            next_ = begin_;
        }

        bool has_next() {
            return next_ != end_;
        }
        operator bool () {
            return has_next();
        }
        Row* next() {
            verify(next_ != end_);
            Row* row = next_->second;
            ++next_;
            return row;
        }
        int count() {
            if (count_ < 0) {
                count_ = 0;
                for (auto it = begin_; it != end_; ++it) {
                    count_++;
                }
            }
            return count_;
        }
    };

    UnsortedTable(std::string name, const Schema* schema): Table(name, schema) {}

    ~UnsortedTable();

    virtual symbol_t rtti() const {
        return TBL_UNSORTED;
    }

    void insert(Row* row) {
        MultiBlob key = row->get_key();
        verify(row->schema() == schema_);
        row->set_table(this);
        insert_into_map(rows_, key, row);
    }

    Cursor query(const Value& kv) {
        return query(kv.get_blob());
    }
    Cursor query(const MultiBlob& key) {
        auto range = rows_.equal_range(key);
        return Cursor(range.first, range.second);
    }
    Cursor all() const {
        return Cursor(std::begin(rows_), std::end(rows_));
    }

    void clear();

    void remove(const Value& kv) {
        remove(kv.get_blob());
    }
    void remove(const MultiBlob& key);
    void remove(Row* row, bool do_free = true);

private:

    iterator remove(iterator it, bool do_free = true);

    // indexed by key values
    std::unordered_multimap<MultiBlob, Row*, MultiBlob::hash> rows_;
};


class RefCountedRow {
    Row* row_;
public:
    RefCountedRow(Row* row): row_(row) {}
    RefCountedRow(const RefCountedRow& r): row_((Row *) r.row_->ref_copy()) {}
    ~RefCountedRow() {
        row_->release();
    }
    const RefCountedRow& operator= (const RefCountedRow& r) {
        if (this != &r) {
            row_->release();
            row_ = (Row *) r.row_->ref_copy();
        }
        return *this;
    }
    Row* get() const {
        return row_;
    }
};


class SnapshotTable: public Table {
    // indexed by key values
    typedef snapshot_sortedmap<SortedMultiKey, RefCountedRow> table_type;
    table_type rows_;

public:

    class Cursor: public Enumerator<const Row*> {
        table_type::range_type* range_;
        table_type::reverse_range_type* reverse_range_;
    public:
        Cursor(const table_type::range_type& range): reverse_range_(nullptr) {
            range_ = new table_type::range_type(range);
        }
        Cursor(const table_type::reverse_range_type& range): range_(nullptr) {
            reverse_range_ = new table_type::reverse_range_type(range);
        }
        ~Cursor() {
            if (range_ != nullptr) {
                delete range_;
            }
            if (reverse_range_ != nullptr) {
                delete reverse_range_;
            }
        }
        virtual bool has_next() {
            if (range_ != nullptr) {
                return range_->has_next();
            } else {
                return reverse_range_->has_next();
            }
        }
        virtual const Row* next() {
            verify(has_next());
            if (range_ != nullptr) {
                return range_->next().second.get();
            } else {
                return reverse_range_->next().second.get();
            }
        }
        int count() {
            if (range_ != nullptr) {
                return range_->count();
            } else {
                return reverse_range_->count();
            }
        }
        bool is_reverse() const {
            return reverse_range_ != nullptr;
        }
        const table_type::range_type& get_range() const {
            return *range_;
        }
        const table_type::reverse_range_type& get_reverse_range() const {
            return *reverse_range_;
        }
    };

    SnapshotTable(std::string name, const Schema* sch): Table(name, sch) {}

    virtual symbol_t rtti() const {
        return TBL_SNAPSHOT;
    }

    SnapshotTable* snapshot() const {
        SnapshotTable* copy = new SnapshotTable(name_, schema_);
        copy->rows_ = rows_.snapshot();
        return copy;
    }

    void insert(Row* row) {
        SortedMultiKey key = SortedMultiKey(row->get_key(), schema_);
        verify(row->schema() == schema_);
        row->set_table(this);

        // make the row readonly, to gaurante snapshot is not changed
        row->make_readonly();

        insert_into_map(rows_, key, RefCountedRow(row));
    }
    Cursor query(const Value& kv) {
        return query(kv.get_blob());
    }
    Cursor query(const MultiBlob& mb) {
        return query(SortedMultiKey(mb, schema_));
    }
    Cursor query(const SortedMultiKey& smk) {
        return Cursor(rows_.query(smk));
    }

    Cursor query_lt(const Value& kv, symbol_t order = symbol_t::ORD_ASC) {
        return query_lt(kv.get_blob(), order);
    }
    Cursor query_lt(const MultiBlob& mb, symbol_t order = symbol_t::ORD_ASC) {
        return query_lt(SortedMultiKey(mb, schema_), order);
    }
    Cursor query_lt(const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC) {
        verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC || order == symbol_t::ORD_ANY);
        if (order == symbol_t::ORD_DESC) {
            return Cursor(rows_.reverse_query_lt(smk));
        } else {
            return Cursor(rows_.query_lt(smk));
        }
    }

    Cursor query_gt(const Value& kv, symbol_t order = symbol_t::ORD_ASC) {
        return query_gt(kv.get_blob(), order);
    }
    Cursor query_gt(const MultiBlob& mb, symbol_t order = symbol_t::ORD_ASC) {
        return query_gt(SortedMultiKey(mb, schema_), order);
    }
    Cursor query_gt(const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC) {
        verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC || order == symbol_t::ORD_ANY);
        if (order == symbol_t::ORD_DESC) {
            return Cursor(rows_.reverse_query_gt(smk));
        } else {
            return Cursor(rows_.query_gt(smk));
        }
    }

    // (low, high) not inclusive
    Cursor query_in(const Value& low, const Value& high, symbol_t order = symbol_t::ORD_ASC) {
        return query_in(low.get_blob(), high.get_blob(), order);
    }
    Cursor query_in(const MultiBlob& low, const MultiBlob& high, symbol_t order = symbol_t::ORD_ASC) {
        return query_in(SortedMultiKey(low, schema_), SortedMultiKey(high, schema_), order);
    }
    Cursor query_in(const SortedMultiKey& low, const SortedMultiKey& high, symbol_t order = symbol_t::ORD_ASC) {
        verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC || order == symbol_t::ORD_ANY);
        verify(low < high);
        if (order == symbol_t::ORD_DESC) {
            return Cursor(rows_.reverse_query_in(low, high));
        } else {
            return Cursor(rows_.query_in(low, high));
        }
    }

    Cursor all(symbol_t order = symbol_t::ORD_ASC) const {
        verify(order == symbol_t::ORD_ASC || order == symbol_t::ORD_DESC || order == symbol_t::ORD_ANY);
        if (order == symbol_t::ORD_DESC) {
            return Cursor(rows_.reverse_all());
        } else {
            return Cursor(rows_.all());
        }
    }

    void clear() {
        rows_ = table_type();
    }

    void remove(const Value& kv) {
        remove(kv.get_blob());
    }
    void remove(const MultiBlob& mb) {
        remove(SortedMultiKey(mb, schema_));
    }
    void remove(const SortedMultiKey& smk) {
        rows_.erase(smk);
    }

    void remove(Row* row, bool do_free = true) {
        verify(row->readonly());   // if the row is in this table, it should've been made readonly
        verify(do_free); // SnapshotTable only allow do_free == true, because there won't be any updates
        SortedMultiKey key = SortedMultiKey(row->get_key(), schema_);
        verify(row->schema() == schema_);
        rows_.erase(key, row);
    }

    void remove(const Cursor& cur) {
        if (cur.is_reverse()) {
            rows_.erase(cur.get_reverse_range());
        } else {
            rows_.erase(cur.get_range());
        }
    }
};

// forward declaration
class IndexedTable;

typedef std::vector<Row*> master_index;

class Index {
    const IndexedTable* idx_tbl_;
    int idx_id_;

    const Schema* get_schema() const;
    SortedTable* get_index_table() const;

public:

    class Cursor: public Enumerator<const Row*> {
        SortedTable::Cursor base_cur_;
    public:
        Cursor(const SortedTable::Cursor& base): base_cur_(base) {}
        void reset() {
            base_cur_.reset();
        }
        bool has_next() {
            return base_cur_.has_next();
        }
        operator bool () {
            return has_next();
        }
        int count() {
            return base_cur_.count();
        }
        const Row* next() {
            Row* index_row = base_cur_.next();
            colid_t last_column_id = index_row->schema()->columns_count() - 1;
            verify(last_column_id >= 0);
            Value pointer_value = index_row->get_column(last_column_id);
            master_index* master_idx = (master_index *) pointer_value.get_i64();
            verify(master_idx != nullptr);
            const Row* base_row = master_idx->back();
            verify(base_row != nullptr);
            return base_row;
        }
    };

    Index(const IndexedTable* idx_tbl, int idx_id): idx_tbl_(idx_tbl), idx_id_(idx_id) {
        verify(idx_id >= 0);
    }

    const IndexedTable* get_table() {
        return idx_tbl_;
    }
    int id() {
        return idx_id_;
    }

    Cursor query(const Value& kv) {
        return query(kv.get_blob());
    }
    Cursor query(const MultiBlob& mb) {
        return query(SortedMultiKey(mb, get_schema()));
    }
    Cursor query(const SortedMultiKey& smk);

    Cursor query_lt(const Value& kv, symbol_t order = symbol_t::ORD_ASC) {
        return query_lt(kv.get_blob(), order);
    }
    Cursor query_lt(const MultiBlob& mb, symbol_t order = symbol_t::ORD_ASC) {
        return query_lt(SortedMultiKey(mb, get_schema()), order);
    }
    Cursor query_lt(const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC);

    Cursor query_gt(const Value& kv, symbol_t order = symbol_t::ORD_ASC) {
        return query_gt(kv.get_blob(), order);
    }
    Cursor query_gt(const MultiBlob& mb, symbol_t order = symbol_t::ORD_ASC) {
        return query_gt(SortedMultiKey(mb, get_schema()), order);
    }
    Cursor query_gt(const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC);

    // (low, high) not inclusive
    Cursor query_in(const Value& low, const Value& high, symbol_t order = symbol_t::ORD_ASC) {
        return query_in(low.get_blob(), high.get_blob(), order);
    }
    Cursor query_in(const MultiBlob& low, const MultiBlob& high, symbol_t order = symbol_t::ORD_ASC) {
        return query_in(SortedMultiKey(low, get_schema()), SortedMultiKey(high, get_schema()), order);
    }
    Cursor query_in(const SortedMultiKey& low, const SortedMultiKey& high, symbol_t order = symbol_t::ORD_ASC);

    Cursor all(symbol_t order = symbol_t::ORD_ASC) const;
};

class IndexedTable: public SortedTable {
    friend class Index;

    // all the secondary indices and their schemas
    std::vector<SortedTable*> indices_;
    std::vector<Schema*> index_schemas_;

    int index_column_id() const {
        return ((IndexedSchema *) schema_)->index_column_id();
    }

    void destroy_secondary_indices(master_index* master_idx);

    virtual iterator remove(iterator it, bool do_free = true);

    Row* make_index_row(Row* base, int idx_id, master_index* master_idx);

public:
    IndexedTable(std::string name, const IndexedSchema* schema);
    ~IndexedTable();

    void insert(Row* row);

    void remove(Index::Cursor idx_cursor);

    // enable searching SortedTable for overloaded `remove` functions
    using SortedTable::remove;

    virtual void notify_before_update(Row* row, int updated_column_id);
    virtual void notify_after_update(Row* row, int updated_column_id);

    Index get_index(int idx_id) const {
        return Index(this, idx_id);
    }
    Index get_index(const std::string& idx_name) const {
        return Index(this, ((IndexedSchema *) schema_)->get_index_id(idx_name));
    }
};

} // namespace mdb
