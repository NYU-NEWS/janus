#pragma once

#include "../__dep__.h"
#include "../classic/tx.h"

namespace janus {

class TxTapir : public TxClassic {
 public:
  set<VersionedRow *> locked_rows_{};
  map<Row *, map<colid_t, mdb::version_t>> read_vers_{};
  map<Row *, map<colid_t, Value>> write_bufs_ = {};
  map<VersionedRow *, map<colid_t, uint64_t>> prepared_rvers_{};
  map<VersionedRow *, map<colid_t, uint64_t>> prepared_wvers_{};

  using TxClassic::TxClassic;

  mdb::Txn *mdb_txn();

  Row *CreateRow(const mdb::Schema *schema,
                 const std::vector<mdb::Value> &values) override {
    return mdb::VersionedRow::create(schema, values);
  }

  virtual bool ReadColumn(Row *row,
                          mdb::colid_t col_id,
                          Value *value,
                          int hint_flag = TXN_SAFE) override;

  virtual bool WriteColumn(Row *row,
                           colid_t col_id,
                           const Value &value,
                           int hint_flag = TXN_SAFE) override;

  virtual ~TxTapir();
};

} // namespace janus
