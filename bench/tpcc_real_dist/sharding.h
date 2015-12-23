#pragma once
#include "__dep__.h"
#include "deptran/sharding.h"

namespace rococo {

class TPCCDSharding: public Sharding {
 public:
  std::multimap<c_last_id_t, rrr::i32> g_c_last2id; // XXX hardcode
  mdb::Schema g_c_last_schema;                      // XXX

  void PopulateTable(tb_info_t *tb_info, uint32_t);
  int PopulateTable(unsigned int sid);
  void temp1(tb_info_t *tb_info,
             uint32_t col_index,
             mdb::Schema::iterator &col_it,
             uint64_t &num_foreign_row,
             std::vector<uint32_t> &bound_foreign_index,
             uint64_t &num_self_primary,
             uint32_t &self_primary_col,
             bool &self_primary_col_find,
             map<uint32_t,
             std::pair<uint32_t, uint32_t> > &prim_foreign_index);
  void temp2(tb_info_t *tb_info,
             uint32_t &sid,
             uint32_t &col_index,
             std::vector<uint32_t> &bound_foreign_index,
             uint64_t &num_self_primary,
             map<uint32_t,
             std::pair<uint32_t, uint32_t> > &prim_foreign_index,
             std::vector<Value> &row_data,
             Value &key_value,
             const mdb::Schema *schema,
             mdb::Table *const table_ptr,
             mdb::SortedTable *tbl_sec_ptr);

};

} // namespace rococo