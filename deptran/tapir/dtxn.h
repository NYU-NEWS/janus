#pragma once

#include "../__dep__.h"
#include "../dtxn.h"

namespace rococo {


class TapirDTxn : public DTxn {
 public:
  map<Row*, map<colid_t, mdb::version_t>> read_vers_ = {};
  map<Row*, map<colid_t, Value>> write_bufs_ = {};

  using DTxn::DTxn;

  mdb::Txn* mdb_txn();

  Row* CreateRow(const mdb::Schema *schema,
                 const std::vector<mdb::Value> &values) {
    return mdb::VersionedRow::create(schema, values);
  }

  bool ReadColumn(mdb::Row *row,
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
                           int hint_flag = TXN_SAFE) override;

  virtual bool WriteColumns(Row *row,
                            const std::vector<colid_t> &col_ids,
                            const std::vector<Value> &values,
                            int hint_flag = TXN_SAFE) override;

  virtual ~TapirDTxn();
};

} // namespace rococo
