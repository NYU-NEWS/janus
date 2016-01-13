#pragma once
#include "__dep__.h"
#include "deptran/sharding.h"

namespace rococo {

class TPCCDSharding: public Sharding {
 public:
  std::multimap<c_last_id_t, rrr::i32> g_c_last2id; // XXX hardcode
  mdb::Schema g_c_last_schema;                      // XXX

  // below is used for table populater

  // number of all combination of foreign columns
  uint64_t num_foreign_row = 1;
  // the column index for foreign w_id and d_id.
  vector<uint32_t> bound_foreign_index = {};
  // the index of column that is primary but not foreign
  uint32_t self_primary_col = 0;
  // col index -> (0, number of records in foreign table or size of value vector)
  map<uint32_t, std::pair<uint32_t, uint32_t> > prim_foreign_index = {};
  uint64_t num_self_primary = 0;
  // the number of row that have been inserted.
  int n_row_inserted_ = 0;
  bool record_key = true; // ?

  void PopulateTable(tb_info_t *tb_info, uint32_t);
  int PopulateTable(unsigned int sid);
  void PreparePrimaryColumn(tb_info_t *tb_info,
                            uint32_t col_index,
                            mdb::Schema::iterator &col_it);
  void InsertRow(tb_info_t *tb_info,
                 uint32_t &sid,
                 Value &key_value,
                 const mdb::Schema *schema,
                 mdb::Table *const table_ptr,
                 mdb::SortedTable *tbl_sec_ptr);

  bool GenerateRowData(tb_info_t *tb_info,
                       uint32_t &sid,
                       Value &key_value,
                       vector<Value> &row_data);

  void InsertRowData(tb_info_t *tb_info,
                     uint32_t &sid,
                     Value &key_value,
                     const mdb::Schema *schema,
                     mdb::Table *const table_ptr,
                     mdb::SortedTable *tbl_sec_ptr,
                     vector<Value> &row_data);

};

} // namespace rococo