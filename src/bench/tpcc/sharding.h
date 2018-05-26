

#pragma once

#include "deptran/__dep__.h"
#include "deptran/sharding.h"

namespace janus {

class TpccSharding : public Sharding {
 public:
  std::multimap<c_last_id_t, rrr::i32> g_c_last2id; // XXX hardcode
  mdb::Schema g_c_last_schema;                      // XXX

  void PreparePrimaryColumn(tb_info_t *tb_info,
                            uint32_t col_index,
                            mdb::Schema::iterator &col_it) override {
    verify(0);
  };
  bool GenerateRowData(tb_info_t *tb_info,
                       uint32_t &sid,
                       Value &key_value,
                       vector<Value> &row_data) override {verify(0);};

  void InsertRowData(tb_info_t *tb_info,
                     uint32_t &partition_id,
                     Value &key_value,
                     const mdb::Schema *schema,
                     mdb::Table *const table_ptr,
                     mdb::SortedTable *tbl_sec_ptr,
                     vector<Value> &row_data) override {verify(0);};

//  bool Ready2Populate(tb_info_t *tb_info) override {verify(0);};


  int PopulateTable(tb_info_t *tb_info, parid_t par_id) override;
  int PopulateTables(parid_t par_id) override;

};

} // namespace janus
