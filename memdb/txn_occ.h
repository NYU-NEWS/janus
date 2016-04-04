#pragma once

#include "txn_2pl.h"

namespace mdb {


class TxnOCC: public Txn2PL {
 public:
  // when ever a read/write is performed, record its version
  // check at commit time if all version values are not changed
  std::unordered_multimap<Row *, column_id_t> locks_;
  std::unordered_map<row_column_pair, version_t, row_column_pair::hash>
      ver_check_read_;
  std::unordered_map<row_column_pair, version_t, row_column_pair::hash>
      ver_check_write_;

  // incr refcount on a Row whenever it gets accessed
  std::set<Row *> accessed_rows_;

  // whether the commit has been verified
  bool verified_;

  // OCC_LAZY: update version only at commit time
  // OCC_EAGER (default): update version at first write (early conflict detection)
  symbol_t policy_;

  std::map<std::string, SnapshotTable *> snapshots_;
  std::set<Table *> snapshot_tables_;

  void incr_row_refcount(Row *r);
  bool version_check();
  bool version_check(const std::unordered_map<row_column_pair,
  version_t,
  row_column_pair::hash> &ver_info);
  void release_resource();

 public:
  TxnOCC(const TxnMgr *mgr, txn_id_t txnid) : Txn2PL(mgr, txnid),
                                              verified_(false),
                                              policy_(symbol_t::OCC_LAZY) { }

  TxnOCC(const TxnMgr *mgr,
         txn_id_t txnid,
         const std::vector<std::string> &table_names);

  ~TxnOCC();

  virtual symbol_t rtti() const {
    return symbol_t::TXN_OCC;
  }

  bool __DebugVersionCheck();
  bool __DebugCheckReadVersion(row_column_pair row_col, version_t ver_now);

  bool is_readonly() const {
    return !snapshot_tables_.empty();
  }
  SnapshotTable *get_snapshot(const std::string &table_name) const {
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

  virtual bool read_column(Row *row, column_id_t col_id, Value *value);
  virtual bool write_column(Row *row, column_id_t col_id, const Value &value);
  virtual bool insert_row(Table *tbl, Row *row);
  virtual bool remove_row(Table *tbl, Row *row);

  using Txn::query;
  using Txn::query_lt;
  using Txn::query_gt;
  using Txn::query_in;

  virtual ResultSet query(Table *tbl, const MultiBlob &mb) {
    verify(
        !is_readonly() || snapshot_tables_.find(tbl) != snapshot_tables_.end());
    return do_query(tbl, mb);
  }
  virtual ResultSet query(Table *tbl, const MultiBlob &mb, bool, int64_t) {
    verify(
        !is_readonly() || snapshot_tables_.find(tbl) != snapshot_tables_.end());
    return do_query(tbl, mb);
  }
  virtual ResultSet query_lt(Table *tbl,
                             const SortedMultiKey &smk,
                             symbol_t order = symbol_t::ORD_ASC) {
    verify(
        !is_readonly() || snapshot_tables_.find(tbl) != snapshot_tables_.end());
    return do_query_lt(tbl, smk, order);
  }
  virtual ResultSet query_gt(Table *tbl,
                             const SortedMultiKey &smk,
                             symbol_t order = symbol_t::ORD_ASC) {
    verify(
        !is_readonly() || snapshot_tables_.find(tbl) != snapshot_tables_.end());
    return do_query_gt(tbl, smk, order);
  }
  virtual ResultSet query_in(Table *tbl,
                             const SortedMultiKey &low,
                             const SortedMultiKey &high,
                             symbol_t order = symbol_t::ORD_ASC) {
    verify(
        !is_readonly() || snapshot_tables_.find(tbl) != snapshot_tables_.end());
    return do_query_in(tbl, low, high, order);
  }
  virtual ResultSet all(Table *tbl, symbol_t order = symbol_t::ORD_ANY) {
    verify(
        !is_readonly() || snapshot_tables_.find(tbl) != snapshot_tables_.end());
    return do_all(tbl, order);
  }
};

class TxnMgrOCC: public TxnMgr {
 public:
  virtual Txn *start(txn_id_t txnid) {
    return new TxnOCC(this, txnid);
  }

  virtual symbol_t rtti() const {
    return symbol_t::TXN_OCC;
  }

  TxnOCC *start_readonly(txn_id_t txnid,
                         const std::vector<std::string> &table_names) {
    return new TxnOCC(this, txnid, table_names);
  }
};

} // namespace mdb