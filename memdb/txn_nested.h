

#pragma once
#include "txn_2pl.h"


namespace mdb {



class TxnNested: public Txn2PL {
  Txn *base_;
  std::unordered_set<Row *> row_inserts_;

 public:
  TxnNested(const TxnMgr *mgr, Txn *base) : Txn2PL(mgr, base->id()),
                                            base_(base) { }

  virtual symbol_t rtti() const {
    return symbol_t::TXN_NESTED;
  }

  virtual void abort();
  virtual bool commit();

  virtual bool read_column(Row *row, column_id_t col_id, Value *value);
  virtual bool write_column(Row *row, column_id_t col_id, const Value &value);
  virtual bool insert_row(Table *tbl, Row *row);
  virtual bool remove_row(Table *tbl, Row *row);

  virtual ResultSet query(Table *tbl, const MultiBlob &mb);
  virtual ResultSet query(Table *tbl, const MultiBlob &mb, bool, int64_t) {
    return query(tbl, mb);
  }
  ResultSet query_lt(Table *tbl,
                     const SortedMultiKey &smk,
                     symbol_t order = symbol_t::ORD_ASC);
  ResultSet query_gt(Table *tbl,
                     const SortedMultiKey &smk,
                     symbol_t order = symbol_t::ORD_ASC);
  ResultSet query_in(Table *tbl,
                     const SortedMultiKey &low,
                     const SortedMultiKey &high,
                     symbol_t order = symbol_t::ORD_ASC);
  ResultSet all(Table *tbl, symbol_t order = symbol_t::ORD_ANY);
};


} // namespace mdb