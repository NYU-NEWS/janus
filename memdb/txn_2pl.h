#pragma once
#include "txn.h"

namespace mdb {

class Txn2PL: public Txn {
 private:
  void release_resource();

 public:
  symbol_t outcome_;
  std::multimap<Row *, column_id_t> reads_;
  std::multimap<Row *, std::pair<column_id_t, Value>> updates_;
  std::multiset<table_row_pair> inserts_;
  std::unordered_set<table_row_pair, table_row_pair::hash> removes_;

  bool debug_check_row_valid(Row *row) const {
    for (auto &it : removes_) {
      if (it.row == row) {
        return false;
      }
    }
    return true;
  }

  ResultSet do_query(Table *tbl, const MultiBlob &mb);

  ResultSet do_query_lt(Table *tbl,
                        const SortedMultiKey &smk,
                        symbol_t order = symbol_t::ORD_ASC);
  ResultSet do_query_gt(Table *tbl,
                        const SortedMultiKey &smk,
                        symbol_t order = symbol_t::ORD_ASC);
  ResultSet do_query_in(Table *tbl,
                        const SortedMultiKey &low,
                        const SortedMultiKey &high,
                        symbol_t order = symbol_t::ORD_ASC);

  ResultSet do_all(Table *tbl, symbol_t order = symbol_t::ORD_ANY);

// public:
//  bool wound_;
//  bool prepared_;


 public:

  Txn2PL() = delete;
  Txn2PL(const TxnMgr *mgr, txn_id_t txnid);
  ~Txn2PL();

  query_buf_t& GetQueryBuf(int64_t);
  virtual bool commit_prepare() {
//    prepared_ = true;
//    if (wound_)
//      return false;
//    else
//      return true;
    verify(0);
    return true;
  }

  virtual symbol_t rtti() const {
    return symbol_t::TXN_2PL;
  }

  virtual void marshal_stage(std::string &str);

  void abort();
  bool commit();

//  void init_piece(i64 tid, i64 pid, rrr::DragonBall *db, mdb::Value *output,
//                  rrr::i32 *output_size);



//    void reg_rm_lock_group(Row *row,
//            const std::function<void(void)> &succ_callback,
//            const std::function<void(void)> &fail_callback);
//
//    void reg_lock_group(const std::vector<column_lock_t> &col_locks,
//            const std::function<void(void)> &succ_callback,
//            const std::function<void(void)> &fail_callback);
//
  void reg_read_column(Row *row,
                       column_id_t col_id,
                       std::function<void(void)> succ_callback,
                       std::function<void(void)> fail_callback);
  void reg_write_column(Row *row,
                        column_id_t col_id,
                        std::function<void(void)> succ_callback,
                        std::function<void(void)> fail_callback);
  virtual bool read_column(Row *row, column_id_t col_id, Value *value);
  virtual bool write_column(Row *row, column_id_t col_id, const Value &value);
  virtual bool insert_row(Table *tbl, Row *row);
  virtual bool remove_row(Table *tbl, Row *row);

  virtual ResultSet query(Table *tbl, const MultiBlob &mb) {
    return do_query(tbl, mb);
  }
  virtual ResultSet query(Table *tbl,
                          const MultiBlob &mb,
                          bool retrieve,
                          int64_t pid);
  virtual ResultSet query_lt(Table *tbl,
                             const SortedMultiKey &smk,
                             symbol_t order = symbol_t::ORD_ASC) {
    return do_query_lt(tbl, smk, order);
  }
  virtual ResultSet query_lt(Table *tbl,
                             const SortedMultiKey &smk,
                             bool retrieve,
                             int64_t pid,
                             symbol_t order = symbol_t::ORD_ASC) {
    query_buf_t &qb = GetQueryBuf(pid);
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
  virtual ResultSet query_gt(Table *tbl,
                             const SortedMultiKey &smk,
                             symbol_t order = symbol_t::ORD_ASC) {
    return do_query_gt(tbl, smk, order);
  }
  virtual ResultSet query_gt(Table *tbl,
                             const SortedMultiKey &smk,
                             bool retrieve,
                             int64_t pid,
                             symbol_t order = symbol_t::ORD_ASC) {
    query_buf_t &qb = GetQueryBuf(pid);
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
  virtual ResultSet query_in(Table *tbl,
                             const SortedMultiKey &low,
                             const SortedMultiKey &high,
                             symbol_t order = symbol_t::ORD_ASC) {
    return do_query_in(tbl, low, high, order);
  }
  virtual ResultSet query_in(Table *tbl,
                             const SortedMultiKey &low,
                             const SortedMultiKey &high,
                             bool retrieve,
                             int64_t pid,
                             symbol_t order = symbol_t::ORD_ASC) {
    query_buf_t &qb = GetQueryBuf(pid);
    if (retrieve) {
      ResultSet rs = qb.buf[qb.retrieve_index++];
      rs.reset();
      return rs;
    } else {
      ResultSet rs = do_query_in(tbl, low, high, order);
      qb.buf.push_back(rs);
      return rs;
    }
  }
  virtual ResultSet all(Table *tbl, symbol_t order = symbol_t::ORD_ANY) {
    return do_all(tbl, order);
  }
  virtual ResultSet all(Table *tbl,
                        bool retrieve,
                        int64_t pid,
                        symbol_t order = symbol_t::ORD_ANY) {
    query_buf_t &qb = GetQueryBuf(pid);
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
  std::multimap<Row *, std::pair<column_id_t, version_t>> vers_;
 public:
  virtual Txn *start(txn_id_t txnid) {
    return new Txn2PL(this, txnid);
  }
  virtual symbol_t rtti() const {
    return symbol_t::TXN_2PL;
  }
};


struct row_column_pair {
  Row *row;
  column_id_t col_id;

  row_column_pair(Row *r, column_id_t c) : row(r), col_id(c) { }

  bool operator==(const row_column_pair &o) const {
    return row == o.row && col_id == o.col_id;
  }

  struct hash {
    size_t operator()(const row_column_pair &p) const {
      size_t v1 = size_t(p.row);
      size_t v2 = size_t(p.col_id);
      return inthash64(v1, v2);
    }
  };
};


} // namespace mdb