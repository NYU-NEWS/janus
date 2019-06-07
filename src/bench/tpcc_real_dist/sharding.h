#pragma once
#include "__dep__.h"
#include "../tpcc/sharding.h"

namespace janus {

class TpccdSharding: public TpccSharding {
 public:


  int PopulateTable(tb_info_t *tb_info, parid_t) override;
  void PreparePrimaryColumn(tb_info_t *tb_info,
                            uint32_t col_index,
                            mdb::Schema::iterator &col_it) override;
  void InsertRow(tb_info_t *tb_info,
                 uint32_t &partition_id,
                 Value &key_value,
                 const mdb::Schema *schema,
                 mdb::Table *const table_ptr,
                 mdb::SortedTable *tbl_sec_ptr) override;

  bool GenerateRowData(tb_info_t *tb_info,
                       uint32_t &sid,
                       Value &key_value,
                       vector<Value> &row_data) override ;

  void InsertRowData(tb_info_t *tb_info,
                     uint32_t &partition_id,
                     Value &key_value,
                     const mdb::Schema *schema,
                     mdb::Table *const table_ptr,
                     mdb::SortedTable *tbl_sec_ptr,
                     vector<Value> &row_data) override ;

};

} // namespace janus
