#pragma once

#include "dtxn.h"

namespace rococo {

class PieceStatus;
class TPLDTxn: public DTxn {
 public:
  vector<mdb::column_lock_t> locks_ = {};
  mdb::Row* row_lock_ = nullptr;
  // true for requiring locks only. false for real execution.
  bool locking_ = false;

  TPLDTxn(epoch_t epoch, txnid_t tid, Scheduler *);


  virtual bool ReadColumn(mdb::Row *row,
                          mdb::column_id_t col_id,
                          Value *value,
                          int hint_flag = TXN_SAFE);

  virtual bool ReadColumns(Row *row,
                           const std::vector<column_id_t> &col_ids,
                           std::vector<Value> *values,
                           int hint_flag = TXN_SAFE);

  virtual bool WriteColumn(Row *row,
                           column_id_t col_id,
                           const Value &value,
                           int hint_flag = TXN_SAFE);

  virtual bool WriteColumns(Row *row,
                            const std::vector<column_id_t> &col_ids,
                            const std::vector<Value> &values,
                            int hint_flag = TXN_SAFE);

  virtual bool InsertRow(Table *tbl, Row *row);
};

} // namespace rococo
