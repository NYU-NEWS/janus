#pragma once

#include "dtxn.h"

namespace rococo {

class PieceStatus;
class TPLDTxn: public DTxn {
 public:

  vector<mdb::column_lock_t> locks_ = {};
  // true for requiring locks only. false for real execution.
  bool locking_ = false;

  TPLDTxn(i64 tid, Scheduler *mgr);

//  int start_launch(
//      const RequestHeader &header,
//      const std::vector <mdb::Value> &input,
//      const rrr::i32 &output_size,
//      rrr::i32 *res,
//      std::vector <mdb::Value> *output,
//      rrr::DeferredReply *defer
//  );


  // This method should not be used for now.
  virtual mdb::Row *create(const mdb::Schema *schema,
                           const std::vector <mdb::Value> &values) {
    verify(0);
    return nullptr;
  }

  virtual bool ReadColumn(mdb::Row *row,
                          mdb::column_id_t col_id,
                          Value *value,
                          bool unsafe = false);

  virtual bool ReadColumns(Row *row,
                           const std::vector<column_id_t> &col_ids,
                           std::vector<Value> *values,
                           bool unsafe = false);

  virtual bool WriteColumn(Row *row,
                           column_id_t col_id,
                           const Value &value,
                           bool unsafe = false);

  virtual bool WriteColumns(Row *row,
                            const std::vector<column_id_t> &col_ids,
                            const std::vector<Value> &values,
                            bool unsafe = false);

//  virtual bool InsertRow(Table *tbl, Row *row);
};

} // namespace rococo
