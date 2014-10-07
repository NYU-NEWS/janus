#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <map>
#include <unordered_set>
#include <set>

#include "utils.h"
#include "value.h"

namespace mdb {

// forward declaration
class Row;
class Table;
class UnsortedTable;
class SortedTable;
class SnapshotTable;
class TxnMgr;
class SortedMultiKey;

typedef i64 txn_id_t;

// forward declaration
class TxnNested;

class ResultSet: public Enumerator<Row*> {
    friend class TxnNested;

    int* refcnt_;
    bool unboxed_;
    Enumerator<const Row*>* rows_;

    void decr_ref() {
        (*refcnt_)--;
        if (*refcnt_ == 0) {
            delete refcnt_;
            if (!unboxed_) {
                delete rows_;
            }
        }
    }

    // only called by TxnNested
    Enumerator<const Row*>* unbox() {
        verify(!unboxed_);
        unboxed_ = true;
        return rows_;
    }

public:
    ResultSet(Enumerator<const Row*>* rows): refcnt_(new int(1)), unboxed_(false), rows_(rows) {}
    ResultSet(const ResultSet& o): refcnt_(o.refcnt_), unboxed_(o.unboxed_), rows_(o.rows_) {
        (*refcnt_)++;
    }
    const ResultSet& operator =(const ResultSet& o) {
        if (this != &o) {
            decr_ref();
            refcnt_ = o.refcnt_;
            unboxed_ = o.unboxed_;
            rows_ = o.rows_;
            (*refcnt_)++;
        }
        return *this;
    }
    ~ResultSet() {
        decr_ref();
    }

    void reset() {
        rows_->reset();
    }

    bool has_next() {
        return rows_->has_next();
    }
    Row* next() {
        return const_cast<Row*>(rows_->next());
    }
};

class Txn: public NoCopy {
protected:
    const TxnMgr* mgr_;
    txn_id_t txnid_;
    Txn(const TxnMgr* mgr, txn_id_t txnid): mgr_(mgr), txnid_(txnid) {}

public:
    virtual ~Txn() {}
    virtual symbol_t rtti() const = 0;
    txn_id_t id() const {
        return txnid_;
    }

    Table* get_table(const std::string& tbl_name) const;
    SortedTable* get_sorted_table(const std::string& tbl_name) const;
    UnsortedTable* get_unsorted_table(const std::string& tbl_name) const;
    SnapshotTable* get_snapshot_table(const std::string& tbl_name) const;

    virtual void abort() = 0;
    virtual bool commit() = 0;

    bool commit_or_abort() {
        bool ret = commit();
        if (!ret) {
            abort();
        }
        return ret;
    }

    virtual bool read_column(Row* row, column_id_t col_id, Value* value) = 0;
    virtual bool write_column(Row* row, column_id_t col_id, const Value& value) = 0;
    virtual bool insert_row(Table* tbl, Row* row) = 0;
    virtual bool remove_row(Table* tbl, Row* row) = 0;

    bool read_columns(Row* row, const std::vector<column_id_t>& col_ids, std::vector<Value>* values) {
        for (auto col_id : col_ids) {
            Value v;
            if (read_column(row, col_id, &v)) {
                values->push_back(v);
            } else {
                return false;
            }
        }
        return true;
    }

    bool write_columns(Row* row, const std::vector<column_id_t>& col_ids, const std::vector<Value>& values) {
        verify(col_ids.size() == values.size());
        for (size_t i = 0; i < col_ids.size(); i++) {
            if (!write_column(row, col_ids[i], values[i])) {
                return false;
            }
        }
        return true;
    }

    ResultSet query(Table* tbl, const Value& kv) {
        return this->query(tbl, kv.get_blob());
    }

    ResultSet query(Table* tbl, const Value& kv, bool retrieve, int64_t pid);

    virtual ResultSet query(Table* tbl, const MultiBlob& mb) = 0;

    virtual ResultSet query(Table* tbl, const MultiBlob& mb, bool retrieve, int64_t pid) {
        verify(rtti() != symbol_t::TXN_2PL);
        return query(tbl, mb);
    }

    ResultSet query_lt(Table* tbl, const Value& kv, symbol_t order = symbol_t::ORD_ASC) {
        return query_lt(tbl, kv.get_blob(), order);
    }
    ResultSet query_lt(Table* tbl, const Value& kv, bool retrieve, int64_t pid, symbol_t order = symbol_t::ORD_ASC) {
        return query_lt(tbl, kv.get_blob(), retrieve, pid, order);
    }
    ResultSet query_lt(Table* tbl, const MultiBlob& mb, symbol_t order = symbol_t::ORD_ASC);
    ResultSet query_lt(Table* tbl, const MultiBlob& mb, bool retrieve, int64_t pid, symbol_t order = symbol_t::ORD_ASC);
    virtual ResultSet query_lt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC) = 0;

    ResultSet query_gt(Table* tbl, const Value& kv, symbol_t order = symbol_t::ORD_ASC) {
        return query_gt(tbl, kv.get_blob(), order);
    }
    ResultSet query_gt(Table* tbl, const Value& kv, bool retrieve, int64_t pid, symbol_t order = symbol_t::ORD_ASC) {
        return query_gt(tbl, kv.get_blob(), retrieve, pid, order);
    }
    ResultSet query_gt(Table* tbl, const MultiBlob& mb, symbol_t order = symbol_t::ORD_ASC);
    ResultSet query_gt(Table* tbl, const MultiBlob& mb, bool retrieve, int64_t pid, symbol_t order = symbol_t::ORD_ASC);
    virtual ResultSet query_gt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC) = 0;

    ResultSet query_in(Table* tbl, const Value& low, const Value& high, symbol_t order = symbol_t::ORD_ASC) {
        return query_in(tbl, low.get_blob(), high.get_blob(), order);
    }
    ResultSet query_in(Table* tbl, const Value& low, const Value& high, bool retrieve, int64_t pid, symbol_t order = symbol_t::ORD_ASC) {
        return query_in(tbl, low.get_blob(), high.get_blob(), retrieve, pid, order);
    }
    ResultSet query_in(Table* tbl, const MultiBlob& low, const MultiBlob& high, symbol_t order = symbol_t::ORD_ASC);
    ResultSet query_in(Table* tbl, const MultiBlob& low, const MultiBlob& high, bool retrieve, int64_t pid, symbol_t order = symbol_t::ORD_ASC);
    virtual ResultSet query_in(Table* tbl, const SortedMultiKey& low, const SortedMultiKey& high, symbol_t order = symbol_t::ORD_ASC) = 0;

    virtual ResultSet all(Table* tbl, symbol_t order = symbol_t::ORD_ANY) = 0;
    virtual ResultSet all(Table* tbl, bool retrieve, int64_t pid, symbol_t order = symbol_t::ORD_ANY);
};


class TxnMgr: public NoCopy {
    std::map<std::string, Table*> tables_;

public:

#ifdef CONFLICT_COUNT
    static std::map<const Table *, uint64_t> vc_conflict_count_;
    static std::map<const Table *, uint64_t> rl_conflict_count_;
    static std::map<const Table *, uint64_t> wl_conflict_count_;
#endif
    virtual ~TxnMgr() {
#ifdef CONFLICT_COUNT
        for (auto it : tables_)
            Log::info("CONFLICT COUNT: Table: %10s,\tversion check: %5llu, read: %5llu\twrite: %5llu", it.first.c_str(), vc_conflict_count_[it.second], rl_conflict_count_[it.second], wl_conflict_count_[it.second]);
#endif
    }
    virtual symbol_t rtti() const = 0;
    virtual Txn* start(txn_id_t txnid) = 0;
    Txn* start_nested(Txn* base);

    void reg_table(const std::string& tbl_name, Table* tbl) {
        verify(tables_.find(tbl_name) == tables_.end());
        insert_into_map(tables_, tbl_name, tbl);
    }

    Table* get_table(const std::string& tbl_name) const {
        auto it = tables_.find(tbl_name);
        if (it == tables_.end()) {
            return nullptr;
        } else {
            return it->second;
        }
    }

    UnsortedTable* get_unsorted_table(const std::string& tbl_name) const;
    SortedTable* get_sorted_table(const std::string& tbl_name) const;
    SnapshotTable* get_snapshot_table(const std::string& tbl_name) const;
};


class TxnUnsafe: public Txn {
public:
    TxnUnsafe(const TxnMgr* mgr, txn_id_t txnid): Txn(mgr, txnid) {}
    virtual symbol_t rtti() const {
        return symbol_t::TXN_UNSAFE;
    }
    void abort() {
        // do nothing
    }
    bool commit() {
        // always allowed
        return true;
    }
    virtual bool read_column(Row* row, column_id_t col_id, Value* value);
    virtual bool write_column(Row* row, column_id_t col_id, const Value& value);
    virtual bool insert_row(Table* tbl, Row* row);
    virtual bool remove_row(Table* tbl, Row* row);

    virtual ResultSet query(Table* tbl, const MultiBlob& mb);

    virtual ResultSet query_lt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC);
    virtual ResultSet query_gt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC);
    virtual ResultSet query_in(Table* tbl, const SortedMultiKey& low, const SortedMultiKey& high, symbol_t order = symbol_t::ORD_ASC);

    virtual ResultSet all(Table* tbl, symbol_t order = symbol_t::ORD_ANY);
};

class TxnMgrUnsafe: public TxnMgr {
public:
    virtual Txn* start(txn_id_t txnid) {
        return new TxnUnsafe(this, txnid);
    }
    virtual symbol_t rtti() const {
        return symbol_t::TXN_UNSAFE;
    }
};


struct table_row_pair {
    Table* table;
    Row* row;

    table_row_pair(Table* t, Row* r): table(t), row(r) {}

    // NOTE: used by set, to do range query in insert_ set
    bool operator < (const table_row_pair& o) const;

    // NOTE: only used by unsorted_set, to lookup in removes_ set
    bool operator == (const table_row_pair& o) const {
        return table == o.table && row == o.row;
    }

    struct hash {
        size_t operator() (const table_row_pair& p) const {
            size_t v1 = size_t(p.table);
            size_t v2 = size_t(p.row);
            return inthash64(v1, v2);
        }
    };

    static Row* ROW_MIN;
    static Row* ROW_MAX;
};

struct column_lock_t {
    Row *row;
    column_id_t column_id;
    rrr::ALock::type_t type;
    column_lock_t(Row *_row, column_id_t _column_id,
            rrr::ALock::type_t _type) :
        row(_row), column_id(_column_id), type(_type) {}
};

struct query_buf_t {
    int retrieve_index;
    std::vector<ResultSet> buf;

    query_buf_t() : retrieve_index(0) {
    }

    query_buf_t(const query_buf_t &o) {
        retrieve_index = o.retrieve_index;
        buf = o.buf;
    }

    const query_buf_t &operator=(const query_buf_t &rhs) {
        retrieve_index = rhs.retrieve_index;
        buf = rhs.buf;
        return *this;
    }
};

class Txn2PL: public Txn {
private:

    void release_piece_map(bool commit);
    void release_resource();

    //rrr::ALockGroup rw_alock_grp_;
    //rrr::ALockGroup rm_alock_grp_;
protected:

    symbol_t outcome_;
    std::multimap<Row*, column_id_t> reads_;
    std::multimap<Row*, std::pair<column_id_t, Value>> updates_;
    std::multiset<table_row_pair> inserts_;
    std::unordered_set<table_row_pair, table_row_pair::hash> removes_;
    //std::unordered_multimap<Row*, std::pair<column_id_t, uint64_t>> alocks_;

    bool debug_check_row_valid(Row* row) const {
        for (auto& it : removes_) {
            if (it.row == row) {
                return false;
            }
        }
        return true;
    }

    ResultSet do_query(Table* tbl, const MultiBlob& mb);

    ResultSet do_query_lt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC);
    ResultSet do_query_gt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC);
    ResultSet do_query_in(Table* tbl, const SortedMultiKey& low, const SortedMultiKey& high, symbol_t order = symbol_t::ORD_ASC);

    ResultSet do_all(Table* tbl, symbol_t order = symbol_t::ORD_ANY);

public:
    class PieceStatus {
        friend class Txn2PL;
        //FIXME when locking thread starts to process a piece, and main thread starts to abort one txn
        // probably we can use a
    private:
        static int64_t swap_bits(int64_t num) {
            int64_t ret = num << 32;
            ret |= (num >> 32) & 0x00000000FFFFFFFF;
            return ret;
        }
        i64 pid_;
        int num_waiting_;
        int num_acquired_;
        bool rej_;
        rrr::DragonBall *reply_db_;
        mdb::Value *output_buf_;
        rrr::i32 *output_size_buf_;
        std::vector<mdb::Value> *output_vec_;
        bool finish_;

        bool rw_succ_;
        rrr::ALockGroup rw_lock_group_;
        bool rm_succ_;
        rrr::ALockGroup rm_lock_group_;
        bool is_rw_;

        bool *wound_;

        query_buf_t query_buf_;

        PieceStatus() : rw_lock_group_(0), rm_lock_group_(0) {
            verify(0);
        }

        PieceStatus(const PieceStatus &ps) : rw_lock_group_(0),
        rm_lock_group_(0) {
            verify(0);
        }

    public:
        PieceStatus(i64 tid, i64 pid, rrr::DragonBall *db, mdb::Value *output,
                rrr::i32 *output_size, bool *wound,
                const std::function<int(void)> &wound_callback) :
            pid_(pid),
            num_waiting_(1),
            num_acquired_(0),
            rej_(false),
            reply_db_(db),
            output_buf_(output),
            output_size_buf_(output_size),
            output_vec_(NULL),
            finish_(false),
            rw_succ_(false),
            rw_lock_group_(swap_bits(tid), wound_callback),
            rm_succ_(false),
            rm_lock_group_(swap_bits(tid), wound_callback),
            is_rw_(false),
            wound_(wound) {
            }

        PieceStatus(i64 tid, i64 pid, rrr::DragonBall *db,
                std::vector<mdb::Value> *output, bool *wound,
                const std::function<int(void)> &wound_callback) :
            pid_(pid),
            num_waiting_(1),
            num_acquired_(0),
            rej_(false),
            reply_db_(db),
            output_buf_(NULL),
            output_size_buf_(NULL),
            output_vec_(output),
            finish_(false),
            rw_succ_(false),
            rw_lock_group_(swap_bits(tid), wound_callback),
            rm_succ_(false),
            rm_lock_group_(swap_bits(tid), wound_callback),
            is_rw_(false),
            wound_(wound) {
            }

        ~PieceStatus() {
        }

        bool is_rejected() {
            return rej_ || *wound_;
        }

        void reg_rm_lock(Row *row,
                const std::function<void(void)> &succ_callback,
                const std::function<void(void)> &fail_callback);

        void reg_rw_lock(const std::vector<column_lock_t> &col_locks,
                const std::function<void(void)> &succ_callback,
                const std::function<void(void)> &fail_callback);

        void abort() {
            verify(finish_);
            if (rw_succ_)
                rw_lock_group_.unlock_all();
            if (rm_succ_)
                rm_lock_group_.unlock_all();
        }

        void commit() {
            verify(finish_);
            if (rw_succ_)
                rw_lock_group_.unlock_all();
        }

        void remove_output() {
            if (output_buf_) {
                verify(output_size_buf_ != NULL);
                *output_size_buf_ = 0;
            }
            else if (output_vec_) {
                output_vec_->resize(0);
            }
        }

        void set_num_waiting_locks(int num) {
            //TODO (right now, only supporting only either rw or delete, not both)
            verify(num <= 1);
            num_waiting_ = num;
            num_acquired_ = 0;
        }

        void start_yes_callback() {
            ps_cache_ = this;
            num_acquired_++;
            verify(num_acquired_ <= num_waiting_);
            if (is_rw_)
                rw_succ_ = true;
            else
                rm_succ_ = true;
        }

        void start_no_callback() {
            ps_cache_ = this;
            num_acquired_++;
            rej_ = true;
            verify(num_acquired_ <= num_waiting_);
        }

        void get_output(std::vector<mdb::Value> **output_vec,
                mdb::Value **output, rrr::i32 **output_size) {
            *output_vec = output_vec_;
            *output = output_buf_;
            *output_size = output_size_buf_;
        }

        bool can_proceed() {
            return num_waiting_ == num_acquired_;
        }

        void trigger_reply_dragonball() {
            reply_db_->trigger();
        }

        void set_finish() {
            verify(!finish_);
            finish_ = true;
        }

    };

private:
    bool wound_, prepared_;

    std::unordered_map<i64, PieceStatus *> piece_map_;

    static PieceStatus *ps_cache_;
public:

    Txn2PL(const TxnMgr* mgr, txn_id_t txnid) :
        Txn(mgr, txnid),
        outcome_(symbol_t::NONE),
        wound_(false),
        prepared_(false) {
        }
    virtual bool commit_prepare() {
        prepared_ = true;
        if (wound_)
            return false;
        else
            return true;
    }
    ~Txn2PL();

    bool is_wound() {
        return wound_;
    }

    static PieceStatus *get_cached_piece_status() {
        return ps_cache_;
    }

    virtual symbol_t rtti() const {
        return symbol_t::TXN_2PL;
    }

    virtual void marshal_stage(std::string &str);

    void abort();
    bool commit();

    void init_piece(i64 tid, i64 pid, rrr::DragonBall *db, mdb::Value* output,
            rrr::i32* output_size);
    void init_piece(i64 tid, i64 pid, rrr::DragonBall *db,
            std::vector<mdb::Value> *output);

    PieceStatus *get_piece_status(i64 pid);

//    void reg_rm_lock_group(Row *row,
//            const std::function<void(void)> &succ_callback,
//            const std::function<void(void)> &fail_callback);
//
//    void reg_lock_group(const std::vector<column_lock_t> &col_locks,
//            const std::function<void(void)> &succ_callback,
//            const std::function<void(void)> &fail_callback);
//
    void reg_read_column(Row *row, column_id_t col_id, std::function<void(void)> succ_callback, std::function<void(void)> fail_callback);
    void reg_write_column(Row *row, column_id_t col_id, std::function<void(void)> succ_callback, std::function<void(void)> fail_callback);
    virtual bool read_column(Row* row, column_id_t col_id, Value* value);
    virtual bool write_column(Row* row, column_id_t col_id, const Value& value);
    virtual bool insert_row(Table* tbl, Row* row);
    virtual bool remove_row(Table* tbl, Row* row);

    virtual ResultSet query(Table* tbl, const MultiBlob& mb) {
        return do_query(tbl, mb);
    }
    virtual ResultSet query(Table* tbl, const MultiBlob& mb, bool retrieve, int64_t pid) {
        query_buf_t &qb = (pid == ps_cache_->pid_) ? ps_cache_->query_buf_
            : piece_map_[pid]->query_buf_;
        if (retrieve) {
            ResultSet rs = qb.buf[qb.retrieve_index++];
            rs.reset();
            return rs;
        }
        else {
            ResultSet rs = do_query(tbl, mb);
            qb.buf.push_back(rs);
            return rs;
        }
    }
    virtual ResultSet query_lt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC) {
        return do_query_lt(tbl, smk, order);
    }
    virtual ResultSet query_lt(Table* tbl, const SortedMultiKey& smk, bool retrieve, int64_t pid, symbol_t order = symbol_t::ORD_ASC) {
        query_buf_t &qb = (pid == ps_cache_->pid_) ? ps_cache_->query_buf_
            : piece_map_[pid]->query_buf_;
        if (retrieve) {
            ResultSet rs = qb.buf[qb.retrieve_index++];
            rs.reset();
            return rs;
        }
        else {
            ResultSet rs = do_query_lt(tbl, smk, order);
            qb.buf.push_back(rs);
            return rs;
        }
    }
    virtual ResultSet query_gt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC) {
        return do_query_gt(tbl, smk, order);
    }
    virtual ResultSet query_gt(Table* tbl, const SortedMultiKey& smk, bool retrieve, int64_t pid, symbol_t order = symbol_t::ORD_ASC) {
        query_buf_t &qb = (pid == ps_cache_->pid_) ? ps_cache_->query_buf_
            : piece_map_[pid]->query_buf_;
        if (retrieve) {
            ResultSet rs = qb.buf[qb.retrieve_index++];
            rs.reset();
            return rs;
        }
        else {
            ResultSet rs = do_query_gt(tbl, smk, order);
            qb.buf.push_back(rs);
            return rs;
        }
    }
    virtual ResultSet query_in(Table* tbl, const SortedMultiKey& low, const SortedMultiKey& high, symbol_t order = symbol_t::ORD_ASC) {
        return do_query_in(tbl, low, high, order);
    }
    virtual ResultSet query_in(Table* tbl, const SortedMultiKey& low, const SortedMultiKey& high, bool retrieve, int64_t pid, symbol_t order = symbol_t::ORD_ASC) {
        query_buf_t &qb = (pid == ps_cache_->pid_) ? ps_cache_->query_buf_
            : piece_map_[pid]->query_buf_;
        if (retrieve) {
            ResultSet rs = qb.buf[qb.retrieve_index++];
            rs.reset();
            return rs;
        }
        else {
            ResultSet rs = do_query_in(tbl, low, high, order);
            qb.buf.push_back(rs);
            return rs;
        }
    }
    virtual ResultSet all(Table* tbl, symbol_t order = symbol_t::ORD_ANY) {
        return do_all(tbl, order);
    }
    virtual ResultSet all(Table* tbl, bool retrieve, int64_t pid, symbol_t order = symbol_t::ORD_ANY) {
        query_buf_t &qb = (pid == ps_cache_->pid_) ? ps_cache_->query_buf_
            : piece_map_[pid]->query_buf_;
        if (retrieve) {
            ResultSet rs = qb.buf[qb.retrieve_index++];
            rs.reset();
            return rs;
        }
        else {
            ResultSet rs = do_all(tbl, order);
            qb.buf.push_back(rs);
            return rs;
        }
    }
};

class TxnMgr2PL: public TxnMgr {
    std::multimap<Row*, std::pair<column_id_t, version_t>> vers_;
public:
    virtual Txn* start(txn_id_t txnid) {
        return new Txn2PL(this, txnid);
    }
    virtual symbol_t rtti() const {
        return symbol_t::TXN_2PL;
    }
};


struct row_column_pair {
    Row* row;
    column_id_t col_id;

    row_column_pair(Row* r, column_id_t c): row(r), col_id(c) {}

    bool operator == (const row_column_pair& o) const {
        return row == o.row && col_id == o.col_id;
    }

    struct hash {
        size_t operator() (const row_column_pair& p) const {
            size_t v1 = size_t(p.row);
            size_t v2 = size_t(p.col_id);
            return inthash64(v1, v2);
        }
    };
};


class TxnOCC: public Txn2PL {
    // when ever a read/write is performed, record its version
    // check at commit time if all version values are not changed
    std::unordered_multimap<Row*, column_id_t> locks_;
    std::unordered_map<row_column_pair, version_t, row_column_pair::hash> ver_check_read_;
    std::unordered_map<row_column_pair, version_t, row_column_pair::hash> ver_check_write_;

    // incr refcount on a Row whenever it gets accessed
    std::set<Row*> accessed_rows_;

    // whether the commit has been verified
    bool verified_;

    // OCC_LAZY: update version only at commit time
    // OCC_EAGER (default): update version at first write (early conflict detection)
    symbol_t policy_;

    std::map<std::string, SnapshotTable*> snapshots_;
    std::set<Table*> snapshot_tables_;

    void incr_row_refcount(Row* r);
    bool version_check();
    bool version_check(const std::unordered_map<row_column_pair, version_t, row_column_pair::hash>& ver_info);
    void release_resource();

public:
    TxnOCC(const TxnMgr* mgr, txn_id_t txnid): Txn2PL(mgr, txnid), verified_(false), policy_(symbol_t::OCC_EAGER) {}

    TxnOCC(const TxnMgr* mgr, txn_id_t txnid, const std::vector<std::string>& table_names);

    ~TxnOCC();

    virtual symbol_t rtti() const {
        return symbol_t::TXN_OCC;
    }

    bool is_readonly() const {
        return !snapshot_tables_.empty();
    }
    SnapshotTable* get_snapshot(const std::string& table_name) const {
        auto it = snapshots_.find(table_name);
        verify(it != snapshots_.end());
        return it->second;
    }

    // call this before ANY operation
    void set_policy(symbol_t policy) {
        verify(policy == symbol_t::OCC_EAGER || policy == symbol_t::OCC_LAZY);
        policy_ = policy;
    }
    symbol_t policy() const {
        return policy_;
    }

    virtual void abort();
    virtual bool commit();

    // for 2 phase commit, prepare will hold writer locks on verified columns,
    // confirm will commit updates and drop those locks
    virtual bool commit_prepare();
    void commit_confirm();

    bool commit_prepare_or_abort() {
        bool ret = commit_prepare();
        if (!ret) {
            abort();
        }
        return ret;
    }

    virtual bool read_column(Row* row, column_id_t col_id, Value* value);
    virtual bool write_column(Row* row, column_id_t col_id, const Value& value);
    virtual bool insert_row(Table* tbl, Row* row);
    virtual bool remove_row(Table* tbl, Row* row);

    using Txn::query;
    using Txn::query_lt;
    using Txn::query_gt;
    using Txn::query_in;

    virtual ResultSet query(Table* tbl, const MultiBlob& mb) {
        verify(!is_readonly() || snapshot_tables_.find(tbl) != snapshot_tables_.end());
        return do_query(tbl, mb);
    }
    virtual ResultSet query(Table *tbl, const MultiBlob &mb, bool, int64_t) {
        verify(!is_readonly() || snapshot_tables_.find(tbl) != snapshot_tables_.end());
        return do_query(tbl, mb);
    }
    virtual ResultSet query_lt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC) {
        verify(!is_readonly() || snapshot_tables_.find(tbl) != snapshot_tables_.end());
        return do_query_lt(tbl, smk, order);
    }
    virtual ResultSet query_gt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC) {
        verify(!is_readonly() || snapshot_tables_.find(tbl) != snapshot_tables_.end());
        return do_query_gt(tbl, smk, order);
    }
    virtual ResultSet query_in(Table* tbl, const SortedMultiKey& low, const SortedMultiKey& high, symbol_t order = symbol_t::ORD_ASC) {
        verify(!is_readonly() || snapshot_tables_.find(tbl) != snapshot_tables_.end());
        return do_query_in(tbl, low, high, order);
    }
    virtual ResultSet all(Table* tbl, symbol_t order = symbol_t::ORD_ANY) {
        verify(!is_readonly() || snapshot_tables_.find(tbl) != snapshot_tables_.end());
        return do_all(tbl, order);
    }
};

class TxnMgrOCC: public TxnMgr {
public:
    virtual Txn* start(txn_id_t txnid) {
        return new TxnOCC(this, txnid);
    }

    virtual symbol_t rtti() const {
        return symbol_t::TXN_OCC;
    }

    TxnOCC* start_readonly(txn_id_t txnid, const std::vector<std::string>& table_names) {
        return new TxnOCC(this, txnid, table_names);
    }
};


class TxnNested: public Txn2PL {
    Txn* base_;
    std::unordered_set<Row*> row_inserts_;

public:
    TxnNested(const TxnMgr* mgr, Txn* base): Txn2PL(mgr, base->id()), base_(base) {}

    virtual symbol_t rtti() const {
        return symbol_t::TXN_NESTED;
    }

    virtual void abort();
    virtual bool commit();

    virtual bool read_column(Row* row, column_id_t col_id, Value* value);
    virtual bool write_column(Row* row, column_id_t col_id, const Value& value);
    virtual bool insert_row(Table* tbl, Row* row);
    virtual bool remove_row(Table* tbl, Row* row);

    virtual ResultSet query(Table* tbl, const MultiBlob& mb);
    virtual ResultSet query(Table* tbl, const MultiBlob& mb, bool, int64_t) {
        return query(tbl, mb);
    }
    ResultSet query_lt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC);
    ResultSet query_gt(Table* tbl, const SortedMultiKey& smk, symbol_t order = symbol_t::ORD_ASC);
    ResultSet query_in(Table* tbl, const SortedMultiKey& low, const SortedMultiKey& high, symbol_t order = symbol_t::ORD_ASC);
    ResultSet all(Table* tbl, symbol_t order = symbol_t::ORD_ANY);
};


} // namespace mdb
