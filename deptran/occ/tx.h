#pragma once

#include "../__dep__.h"
#include "../dtxn.h"

namespace rococo {

class TxOcc : public Tx {
public:
  using Tx::Tx;

  virtual bool ReadColumn(mdb::Row *row,
                          mdb::colid_t col_id,
                          Value *value,
                          int hint_flag = TXN_SAFE) override;

  virtual bool ReadColumns(Row *row,
                           const std::vector<colid_t> &col_ids,
                           std::vector<Value> *values,
                           int hint_flag = TXN_SAFE) override;

  virtual bool WriteColumn(Row *row,
                           colid_t col_id,
                           const Value &value,
                           int hint_flag = TXN_SAFE);

  virtual bool WriteColumns(Row *row,
                            const std::vector<colid_t> &col_ids,
                            const std::vector<Value> &values,
                            int hint_flag = TXN_SAFE);

  virtual bool InsertRow(Table *tbl, Row *row);
};

} // namespace rococo