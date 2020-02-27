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

class ResultSet: public Enumerator<Row *> {
  friend class TxnNested;

  int *refcnt_;
  bool unboxed_;
  Enumerator<const Row *> *rows_;

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
  Enumerator<const Row *> *unbox() {
    verify(!unboxed_);
    unboxed_ = true;
    return rows_;
  }

 public:
  ResultSet(Enumerator<const Row *> *rows)
      : refcnt_(new int(1)), unboxed_(false), rows_(rows) { }
  ResultSet(const ResultSet &o)
      : refcnt_(o.refcnt_), unboxed_(o.unboxed_), rows_(o.rows_) {
    (*refcnt_)++;
  }
  const ResultSet &operator=(const ResultSet &o) {
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
  Row *next() {
    return const_cast<Row *>(rows_->next());
  }
};

class Txn: public NoCopy {
 public:
  const TxnMgr *mgr_;
  txn_id_t txnid_;
  Txn(const TxnMgr *mgr, txn_id_t txnid) : mgr_(mgr), txnid_(txnid) { }

 public:
  virtual ~Txn() { }
  virtual symbol_t rtti() const = 0;
  txn_id_t id() const {
    return txnid_;
  }

  Table *get_table(const std::string &tbl_name) const;
  SortedTable *get_sorted_table(const std::string &tbl_name) const;
  UnsortedTable *get_unsorted_table(const std::string &tbl_name) const;
  SnapshotTable *get_snapshot_table(const std::string &tbl_name) const;

  virtual void abort() = 0;
  virtual bool commit() = 0;

  bool commit_or_abort() {
    bool ret = commit();
    if (!ret) {
      abort();
    }
    return ret;
  }

  virtual bool read_column(Row *row, colid_t col_id, Value *value) = 0;
  virtual bool
      write_column(Row *row, colid_t col_id, const Value &value) = 0;
  virtual bool insert_row(Table *tbl, Row *row) = 0;
  virtual bool remove_row(Table *tbl, Row *row) = 0;

  bool read_columns(Row *row,
                    const std::vector<colid_t> &col_ids,
                    std::vector<Value> *values) {
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

  bool write_columns(Row *row,
                     const std::vector<colid_t> &col_ids,
                     const std::vector<Value> &values) {
    verify(col_ids.size() == values.size());
    for (size_t i = 0; i < col_ids.size(); i++) {
      if (!write_column(row, col_ids[i], values[i])) {
        return false;
      }
    }
    return true;
  }

  ResultSet query(Table *tbl, const Value &kv) {
    return this->query(tbl, kv.get_blob());
  }

  ResultSet query(Table *tbl, const Value &kv, bool retrieve, int64_t pid);

  virtual ResultSet query(Table *tbl, const MultiBlob &mb) = 0;

  virtual ResultSet query(Table *tbl,
                          const MultiBlob &mb,
                          bool retrieve,
                          int64_t pid) {
    verify(rtti() != symbol_t::TXN_2PL);
    return query(tbl, mb);
  }

  ResultSet query_lt(Table *tbl,
                     const Value &kv,
                     symbol_t order = symbol_t::ORD_ASC) {
    return query_lt(tbl, kv.get_blob(), order);
  }
  ResultSet query_lt(Table *tbl,
                     const Value &kv,
                     bool retrieve,
                     int64_t pid,
                     symbol_t order = symbol_t::ORD_ASC) {
    return query_lt(tbl, kv.get_blob(), retrieve, pid, order);
  }
  ResultSet query_lt
      (Table *tbl, const MultiBlob &mb, symbol_t order = symbol_t::ORD_ASC);
  ResultSet query_lt(Table *tbl,
                     const MultiBlob &mb,
                     bool retrieve,
                     int64_t pid,
                     symbol_t order = symbol_t::ORD_ASC);
  virtual ResultSet query_lt(Table *tbl,
                             const SortedMultiKey &smk,
                             symbol_t order = symbol_t::ORD_ASC) = 0;

  ResultSet query_gt(Table *tbl,
                     const Value &kv,
                     symbol_t order = symbol_t::ORD_ASC) {
    return query_gt(tbl, kv.get_blob(), order);
  }
  ResultSet query_gt(Table *tbl,
                     const Value &kv,
                     bool retrieve,
                     int64_t pid,
                     symbol_t order = symbol_t::ORD_ASC) {
    return query_gt(tbl, kv.get_blob(), retrieve, pid, order);
  }
  ResultSet query_gt(Table *tbl,
                     const MultiBlob &mb,
                     symbol_t order = symbol_t::ORD_ASC);
  ResultSet query_gt(Table *tbl,
                     const MultiBlob &mb,
                     bool retrieve,
                     int64_t pid,
                     symbol_t order = symbol_t::ORD_ASC);
  virtual ResultSet query_gt(Table *tbl,
                             const SortedMultiKey &smk,
                             symbol_t order = symbol_t::ORD_ASC) = 0;

  ResultSet query_in(Table *tbl,
                     const Value &low,
                     const Value &high,
                     symbol_t order = symbol_t::ORD_ASC) {
    return query_in(tbl, low.get_blob(), high.get_blob(), order);
  }
  ResultSet query_in(Table *tbl,
                     const Value &low,
                     const Value &high,
                     bool retrieve,
                     int64_t pid,
                     symbol_t order = symbol_t::ORD_ASC) {
    return query_in(tbl, low.get_blob(), high.get_blob(), retrieve, pid, order);
  }
  ResultSet query_in(Table *tbl,
                     const MultiBlob &low,
                     const MultiBlob &high,
                     symbol_t order = symbol_t::ORD_ASC);
  ResultSet query_in(Table *tbl,
                     const MultiBlob &low,
                     const MultiBlob &high,
                     bool retrieve,
                     int64_t pid,
                     symbol_t order = symbol_t::ORD_ASC);
  virtual ResultSet query_in(Table *tbl,
                             const SortedMultiKey &low,
                             const SortedMultiKey &high,
                             symbol_t order = symbol_t::ORD_ASC) = 0;

  virtual ResultSet all(Table *tbl, symbol_t order = symbol_t::ORD_ANY) = 0;
  virtual ResultSet all(Table *tbl,
                        bool retrieve,
                        int64_t pid,
                        symbol_t order = symbol_t::ORD_ANY);
};


class TxnMgr: public NoCopy {
  std::map<std::string, Table *> tables_;

 public:

#ifdef CONFLICT_COUNT
//  static std::map<const Table *, uint64_t> vc_conflict_count_;
//  static std::map<const Table *, uint64_t> rl_conflict_count_;
//  static std::map<const Table *, uint64_t> wl_conflict_count_;
#endif
  virtual ~TxnMgr() {
#ifdef CONFLICT_COUNT
    for (auto it : tables_)
        Log::info("CONFLICT COUNT: Table: %10s,\tversion check: %5llu, read: %5llu\twrite: %5llu", it.first.c_str(), vc_conflict_count_[it.second], rl_conflict_count_[it.second], wl_conflict_count_[it.second]);
#endif
  }
  virtual symbol_t rtti() const = 0;
  virtual Txn *start(txn_id_t txnid) = 0;
  Txn *start_nested(Txn *base);

  uint16_t Checksum() {
    uint16_t result = 0;
    for (auto pair : tables_) {
      auto tbl = pair.second;
      result ^= tbl->Checksum();
    }
    return result;
  }

  void reg_table(const std::string &tbl_name, Table *tbl) {
    verify(tables_.find(tbl_name) == tables_.end());
    insert_into_map(tables_, tbl_name, tbl);
  }

  Table *get_table(const std::string &tbl_name) const {
    auto it = tables_.find(tbl_name);
    if (it == tables_.end()) {
      return nullptr;
    } else {
      return it->second;
    }
  }

  UnsortedTable *get_unsorted_table(const std::string &tbl_name) const;
  SortedTable *get_sorted_table(const std::string &tbl_name) const;
  SnapshotTable *get_snapshot_table(const std::string &tbl_name) const;
};



struct table_row_pair {
  Table *table;
  Row *row;

  table_row_pair(Table *t, Row *r) : table(t), row(r) { }

  // NOTE: used by set, to do range query in insert_ set
  bool operator<(const table_row_pair &o) const;

  // NOTE: only used by unsorted_set, to lookup in removes_ set
  bool operator==(const table_row_pair &o) const {
    return table == o.table && row == o.row;
  }

  struct hash {
    size_t operator()(const table_row_pair &p) const {
      size_t v1 = size_t(p.table);
      size_t v2 = size_t(p.row);
      return inthash64(v1, v2);
    }
  };

  static Row *ROW_MIN; // Multi thread safe? TODO
  static Row *ROW_MAX; // Multi thread safe? TODO
};

struct column_lock_t {
  Row *row;
  colid_t column_id;
  rrr::ALock::type_t type;
  column_lock_t(Row *_row, colid_t _column_id,
                rrr::ALock::type_t _type) :
      row(_row), column_id(_column_id), type(_type) { }
};

struct query_buf_t {
  int retrieve_index;
  std::vector<ResultSet> buf;

  query_buf_t() : retrieve_index(0), buf(std::vector<ResultSet>()) {
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

// for helping locating in inserts set
class KeyOnlySearchRow: public Row {
  const MultiBlob *mb_;
 public:
  KeyOnlySearchRow(const Schema *schema, const MultiBlob *mb) : mb_(mb) {
    schema_ = schema;
  }
  virtual MultiBlob get_key() const {
    return *mb_;
  }
};


// merge query result in staging area and real table data
class MergedCursor: public NoCopy, public Enumerator<const Row *> {
  Table *tbl_;
  Enumerator<const Row *> *cursor_;

  bool reverse_order_;
  std::multiset<table_row_pair>::const_iterator inserts_next_, inserts_end_;
  std::multiset<table_row_pair>::const_reverse_iterator r_inserts_next_,
      r_inserts_end_;

  const std::unordered_set<table_row_pair, table_row_pair::hash> &removes_;

  bool cached_;
  const Row *cached_next_;
  const Row *next_candidate_;

  void insert_reset() {
    //TODO it's OK for the current workload
  }

  bool insert_has_next() {
    if (reverse_order_) {
      return r_inserts_next_ != r_inserts_end_;
    } else {
      return inserts_next_ != inserts_end_;
    }
  }

  const Row *insert_get_next() {
    if (reverse_order_) {
      return r_inserts_next_->row;
    } else {
      return inserts_next_->row;
    }
  }

  void insert_advance_next() {
    if (reverse_order_) {
      ++r_inserts_next_;
    } else {
      ++inserts_next_;
    }
  }

  bool prefetch_next() {
    verify(cached_ == false);

    while (next_candidate_ == nullptr && cursor_->has_next()) {
      next_candidate_ = cursor_->next();

      // check if row has been removeds
      table_row_pair needle(tbl_, const_cast<Row *>(next_candidate_));
      if (removes_.find(needle) != removes_.end()) {
        next_candidate_ = nullptr;
      }
    }

    // check if there's data in inserts_
    if (next_candidate_ == nullptr) {
      if (insert_has_next()) {
        cached_ = true;
        cached_next_ = insert_get_next();
        insert_advance_next();
      }
    } else {
      // next_candidate_ != nullptr
      // check which is next: next_candidate_, or next in inserts_
      cached_ = true;
      if (insert_has_next()) {
        if (next_candidate_ < insert_get_next()) {
          cached_next_ = next_candidate_;
          next_candidate_ = nullptr;
        } else {
          cached_next_ = insert_get_next();
          insert_advance_next();
        }
      } else {
        cached_next_ = next_candidate_;
        next_candidate_ = nullptr;
      }
    }

    return cached_;
  }

 public:
  MergedCursor(Table *tbl,
               Enumerator<const Row *> *cursor,
               const std::multiset<table_row_pair>::const_iterator &inserts_begin,
               const std::multiset<table_row_pair>::const_iterator &inserts_end,
               const std::unordered_set<table_row_pair,
                                        table_row_pair::hash> &removes)
      : tbl_(tbl), cursor_(cursor), reverse_order_(false),
        inserts_next_(inserts_begin), inserts_end_(inserts_end),
        removes_(removes),
        cached_(false), cached_next_(nullptr), next_candidate_(nullptr) { }

  MergedCursor(Table *tbl,
               Enumerator<const Row *> *cursor,
               const std::multiset<table_row_pair>::const_reverse_iterator &inserts_rbegin,
               const std::multiset<table_row_pair>::const_reverse_iterator &inserts_rend,
               const std::unordered_set<table_row_pair,
                                        table_row_pair::hash> &removes)
      : tbl_(tbl), cursor_(cursor), reverse_order_(true),
        r_inserts_next_(inserts_rbegin), r_inserts_end_(inserts_rend),
        removes_(removes),
        cached_(false), cached_next_(nullptr), next_candidate_(nullptr) { }

  ~MergedCursor() {
    delete cursor_;
  }

  void reset() {
    insert_reset();
    cursor_->reset();
  }

  bool has_next() {
    if (cached_) {
      return true;
    } else {
      return prefetch_next();
    }
  }

  const Row *next() {
    if (!cached_) {
      verify(prefetch_next());
    }
    cached_ = false;
    return cached_next_;
  }
};

static void redirect_locks(std::unordered_multimap<Row *, colid_t> &locks,
                           Row *new_row,
                           Row *old_row) {
  auto it_pair = locks.equal_range(old_row);
  std::vector<colid_t> locked_columns;
  for (auto it_lock = it_pair.first; it_lock != it_pair.second; ++it_lock) {
    locked_columns.push_back(it_lock->second);
  }
  if (!locked_columns.empty()) {
    locks.erase(old_row);
  }
  for (auto &col_id : locked_columns) {
    insert_into_map(locks, new_row, col_id);
  }
}


} // namespace mdb
