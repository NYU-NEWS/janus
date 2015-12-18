#pragma once

#include "dtxn.h"

namespace rococo {

class PieceStatus;
class TPLDTxn: public DTxn {
 public:


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

  virtual bool ReadColumn(
      mdb::Row *row,
      mdb::column_id_t col_id,
      Value *value
  );
};

} // namespace rococo
