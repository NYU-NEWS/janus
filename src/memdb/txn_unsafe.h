#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <map>
#include <unordered_set>
#include <set>

#include "utils.h"
#include "value.h"
#include "txn.h"

namespace mdb {

class TxnUnsafe: public Txn {
 public:
  virtual symbol_t rtti() const {
    return symbol_t::TXN_UNSAFE;
  }

  TxnUnsafe(const TxnMgr *mgr, txn_id_t txnid) : Txn(mgr, txnid) { }
  virtual ~TxnUnsafe(){}

  void abort() {
    // do nothing
  }
  bool commit() {
    // always allowed
    return true;
  }
  virtual bool read_column(Row *row, colid_t col_id, Value *value);
  virtual bool write_column(Row *row, colid_t col_id, const Value &value);
  virtual bool insert_row(Table *tbl, Row *row);
  virtual bool remove_row(Table *tbl, Row *row);

  virtual ResultSet query(Table *tbl, const MultiBlob &mb);

  virtual ResultSet query_lt(Table *tbl,
                             const SortedMultiKey &smk,
                             symbol_t order = symbol_t::ORD_ASC);
  virtual ResultSet query_gt(Table *tbl,
                             const SortedMultiKey &smk,
                             symbol_t order = symbol_t::ORD_ASC);
  virtual ResultSet query_in(Table *tbl,
                             const SortedMultiKey &low,
                             const SortedMultiKey &high,
                             symbol_t order = symbol_t::ORD_ASC);

  virtual ResultSet all(Table *tbl, symbol_t order = symbol_t::ORD_ANY);
};

class TxnMgrUnsafe: public TxnMgr {
 public:
  virtual Txn *start(txn_id_t txnid) {
    return new TxnUnsafe(this, txnid);
  }
  virtual symbol_t rtti() const {
    return symbol_t::TXN_UNSAFE;
  }
};



} // namespace mdb
