#include "__dep__.h"
#include "constants.h"
#include "config.h"
#include "tx.h"
#include "bench/tpcc_real_dist/sharding.h"
#include "bench/tpcc/workload.h"
#include "frame.h"
#include "scheduler.h"

namespace janus {

void TpccdSharding::InsertRowData(tb_info_t *tb_info,
                                  uint32_t &partition_id,
                                  Value &key_value,
                                  const mdb::Schema *schema,
                                  mdb::Table *const table_ptr,
                                  mdb::SortedTable *tbl_sec_ptr,
                                  vector<Value> &row_data) {
  verify(frame_ != nullptr);
  if (tb_info->tb_name == TPCC_TB_STOCK) {
    size_t key_size = schema->key_columns_id().size();
    MultiValue mv(key_size);

    for (int i = 0; i < key_size; i++) {
      mv[i] = row_data[schema->key_columns_id()[i]];
    }

    if (partition_id == PartitionFromKey(mv, tb_info)) {
      n_row_inserted_++;
      auto r = frame_->CreateRow(schema, row_data);
      table_ptr->insert(r);
    }
  } else if (tb_info->tb_name == TPCC_TB_DISTRICT) {
    size_t key_size = schema->key_columns_id().size();
    MultiValue mv(key_size);

    for (int i = 0; i < key_size; i++) {
      mv[i] = row_data[schema->key_columns_id()[i]];
    }

    if (partition_id == PartitionFromKey(mv, tb_info)) {
      n_row_inserted_++;
      auto r = frame_->CreateRow(schema, row_data);
      table_ptr->insert(r);

      for (int i = 0; i < key_size; i++) {
        tb_info->columns[schema->key_columns_id()[i]].values->push_back(mv[i]);
      }
    }
  } else {
    n_row_inserted_++;
    auto r = frame_->CreateRow(schema, row_data);
    table_ptr->insert(r);

    // XXX order (o_d_id, o_w_id, o_c_id) --> maximum o_id
    if (tbl_sec_ptr) {
      int32_t cur_o_id_buf = r->get_column("o_id").get_i32();
      const mdb::Schema *sch_buf = tbl_sec_ptr->schema();
      mdb::MultiBlob mb_buf(sch_buf->key_columns_id().size());
      mdb::Schema::iterator col_info_it = sch_buf->begin();
      size_t mb_i = 0;

      for (; col_info_it != sch_buf->end(); col_info_it++) {
        if (col_info_it->indexed) {
          mb_buf[mb_i++] = r->get_blob(col_info_it->name);
        }
      }
      mdb::SortedTable::Cursor rs = tbl_sec_ptr->query(mb_buf);

      if (rs.has_next()) {
        mdb::Row *r_buf = rs.next();
        int32_t o_id_buf = r_buf->get_column("o_id").get_i32();

        if (o_id_buf < cur_o_id_buf) {
          r_buf->update("o_id", cur_o_id_buf);
        }
      } else {
        std::vector<Value> sec_row_data_buf;

        for (col_info_it = sch_buf->begin();
             col_info_it != sch_buf->end();
             col_info_it++) {
          sec_row_data_buf.push_back(r->get_column(col_info_it->name));
        }
        auto r = frame_->CreateRow(sch_buf, sec_row_data_buf);
        tbl_sec_ptr->insert(r);
      }
    }

    // XXX c_last secondary index
    if (tb_info->tb_name == TPCC_TB_CUSTOMER) {
      std::string c_last_buf = r->get_column("c_last").get_str();
      int32_t c_id_buf = r->get_column("c_id").get_i32();
      size_t mb_size = g_c_last_schema.key_columns_id().size();
      size_t mb_i = 0;
      mdb::MultiBlob mb_buf(mb_size);
      mdb::Schema::iterator col_info_it = g_c_last_schema.begin();

      for (; col_info_it != g_c_last_schema.end(); col_info_it++) {
        mb_buf[mb_i++] = r->get_blob(col_info_it->name);
      }
      g_c_last2id.insert(std::make_pair(c_last_id_t(c_last_buf,
                                                    mb_buf,
                                                    &g_c_last_schema),
                                        c_id_buf));
    }
  }
}

bool TpccdSharding::GenerateRowData(tb_info_t *tb_info,
                                    uint32_t &sid,
                                    Value &key_value,
                                    vector<Value> &row_data) {
  size_t col_index = 0;
  for (; col_index < tb_info->columns.size(); col_index++) {
    auto &col = tb_info->columns[col_index];
    if (col.is_primary) {
      // primary key(column)
      if (prim_foreign_index.size() == 0) {
        // primary key, and no column is foreign key.
        if ((tb_info->tb_name != TPCC_TB_WAREHOUSE) &&
            (tb_info->tb_name != TPCC_TB_ITEM) &&
            (sid != PartitionFromKey(key_value, tb_info))) {
          // the key does not belong to this site.
          break;
        }
        row_data.push_back(key_value);

        if (col.values != NULL) {
          col.values->push_back(key_value);
        }
      } else if (col.foreign != NULL) {
        // primary key, and this is a foreign key
        Value v_buf;
        int xxx = prim_foreign_index[col_index].first;
        if ((col.foreign->name == "i_id") ||
            (col.foreign->name == "w_id")) {
          v_buf = Value((i32) xxx);
        } else {
          v_buf = (*col.foreign->values)[xxx];
        }
        row_data.push_back(v_buf);
      } else {
        // primary key, is not foreign, other col is foreign.
        row_data.push_back(key_value);

        if ((col.values != NULL) &&
            (tb_info->tb_name != TPCC_TB_DISTRICT) &&
            record_key) {
          col.values->push_back(key_value);
          record_key = false;
        }
      }
    } else if (col.foreign != NULL) {
      // not primary, but is foreign
      bool use_key_value = false;
      uint64_t n;
      size_t fc_size = col.foreign->name.size();
      std::string last4 = col.foreign->name.substr(fc_size - 4, 4);
      verify(col.foreign_col_name == col.foreign->name);
      if (boost::algorithm::ends_with(col.foreign_col_name, "i_id")) {
        n = tb_infos_[std::string(TPCC_TB_ITEM)].num_records;
      } else if (boost::algorithm::ends_with(col.foreign_col_name, "w_id")) {
        n = tb_infos_[std::string(TPCC_TB_WAREHOUSE)].num_records;
      } else if (boost::algorithm::ends_with(col.foreign_col_name, "c_id")) {
        if (col.name == "o_c_id") {
          use_key_value = true;
        } else {
          n = tb_infos_[std::string(TPCC_TB_CUSTOMER)].num_records /
              tb_infos_[std::string(TPCC_TB_DISTRICT)].num_records;
        }
      } else if (boost::algorithm::ends_with(col.foreign_col_name, "d_id")) {
        n = Config::GetConfig()->GetNumPartition() *
            tb_infos_[std::string(TPCC_TB_DISTRICT)].num_records /
            tb_infos_[std::string(TPCC_TB_WAREHOUSE)].num_records;
      } else {
        auto ftbl = col.foreign_tb;
        verify(ftbl);
        n = ftbl->num_records;
      }
      Value v_buf;

      if (use_key_value) {
        v_buf = key_value;
      } else {
        v_buf = value_get_n(col.type, RandomGenerator::rand(0, n - 1));
      }
      row_data.push_back(v_buf);
    } else {
      // neither primary nor foreign.
      Value v_buf = random_value(col.type);
      if (col.name == "d_next_o_id") {
        v_buf = Value((i32) (
            tb_infos_[string(TPCC_TB_ORDER)].num_records /
                tb_infos_[string(TPCC_TB_DISTRICT)].num_records)); // XXX
      }

      if (col.name == "c_last") {
        v_buf = Value(RandomGenerator::int2str_n(
            key_value.get_i32() % 1000, 3));
      }
      row_data.push_back(v_buf);
    }
  }
  return col_index == tb_info->columns.size();
}

void TpccdSharding::InsertRow(tb_info_t *tb_info,
                              uint32_t &partition_id,
                              Value &key_value,
                              const mdb::Schema *schema,
                              mdb::Table *const table_ptr,
                              mdb::SortedTable *tbl_sec_ptr) {
  vector<Value> row_data;
  row_data.reserve(tb_info->columns.size());
  record_key = true;
  n_row_inserted_ = 0;
  while (true) {
    row_data.clear();
    // generate row data
    auto ret = GenerateRowData(tb_info, partition_id, key_value, row_data);
    if (ret) {
      // if the row_data is successfully generated.
      InsertRowData(tb_info, partition_id, key_value, schema,
                    table_ptr, tbl_sec_ptr, row_data);
      if ((num_self_primary == 0) && (n_row_inserted_ >= tb_info->num_records)) {
        // enough records have been inserted, give the caller the hint.
        num_self_primary = 1;
        break;
      }
    } else {
      // if the row data is not generated, do nothing.
    }

    if (num_self_primary == 0) {
      if (0 != index_increase(prim_foreign_index, bound_foreign_index)) {
        verify(0);
      }
    } else if (0 != index_increase(prim_foreign_index, bound_foreign_index)) {
      break;
    }
  }
}

// if a column is primary, this should be called.
void TpccdSharding::PreparePrimaryColumn(tb_info_t *tb_info,
                                         uint32_t col_index,
                                         mdb::Schema::iterator &col_it) {
  auto &col = tb_info->columns[col_index];
  if (!col.is_primary) {
    return;
  }
  verify(col_it->indexed);

  if (tb_info->tb_name == TPCC_TB_CUSTOMER &&
      col_it->name != "c_id") {
    // XXX add all columns except c_id?
    // should be c_w_id and c_d_id
    g_c_last_schema.add_column(col_it->name.c_str(), col_it->type, true);
  }

  column_t *foreign_column = col.foreign;
  if (col.foreign_tb != NULL) {
    uint64_t tmp_index_base;
    bool times_tmp_int = true;

    uint64_t tmp_int = col.foreign_tb->num_records;
    if (foreign_column->name == "i_id") {
      // refers to item.i_id, use all available i_id instead
      // of local i_id
      tmp_index_base = tmp_int;

      if (col.values != NULL) {
        delete col.values;
        col.values = NULL;
      }
    } else if (foreign_column->name == "w_id") {
      // refers to warehouse.w_id, use all available w_id instead
      // of local w_id
      tmp_index_base = tmp_int;

      if (col.values != NULL) {
        if (tb_info->tb_name == TPCC_TB_STOCK) {
          delete col.values;
          col.values = NULL;
        } else {
          verify(tb_info->tb_name == TPCC_TB_DISTRICT);
        }
      }
    } else {
      times_tmp_int = false;

      if (num_foreign_row == 1) {
        num_foreign_row *= tmp_int;
      }
      verify(foreign_column->values);
      tmp_index_base = foreign_column->values->size();
      if (boost::algorithm::ends_with(foreign_column->name, "w_id") ||
          boost::algorithm::ends_with(foreign_column->name, "d_id")) {
        if (!bound_foreign_index.empty()) {
          auto xxx = prim_foreign_index[bound_foreign_index[0]].second;
          verify(tmp_index_base == xxx);
        }
        bound_foreign_index.push_back(col_index);
      }

      if (col.values != NULL) {
        col.values->assign(foreign_column->values->begin(),
                           foreign_column->values->end());
      }
    }
    verify(tmp_index_base > 0);
    prim_foreign_index[col_index] = std::make_pair(0, tmp_index_base);

    if (times_tmp_int) {
      num_foreign_row *= tmp_int;
    }
  } else {
    // this primary key is not foreign key. Currently we only support
    // at most one column that is primary but not foreign.
    self_primary_col = col_index;
    if (col.name == "i_id" || col.name == "w_id") {
      if (col.values != NULL) {
        delete col.values;
        col.values = NULL;
      }
    }
  }
}

int TpccdSharding::PopulateTable(tb_info_t *tb_info, parid_t partition_id) {
  // find table and secondary table
  mdb::Table *const table_ptr = tx_sched_->get_table(tb_info->tb_name);
  const mdb::Schema *schema = table_ptr->schema();
  mdb::SortedTable *tbl_sec_ptr = nullptr;

  if (tb_info->tb_name == TPCC_TB_ORDER) {
    tbl_sec_ptr = (mdb::SortedTable *) tx_sched_->get_table(
            TPCC_TB_ORDER_C_ID_SECONDARY);
  }
  verify(schema->columns_count() == tb_info->columns.size());

  num_foreign_row = 1;
  bound_foreign_index = {};
  self_primary_col = 0;
  prim_foreign_index = {};
  uint32_t col_index = 0;

  mdb::Schema::iterator col_it = schema->begin();
  for (col_index = 0; col_index < tb_info->columns.size(); col_index++) {
    verify(col_it != schema->end());
    verify(tb_info->columns[col_index].name == col_it->name);
    verify(tb_info->columns[col_index].type == col_it->type);
    PreparePrimaryColumn(tb_info, col_index, col_it);
    col_it++;
  }

  if (tb_info->tb_name == TPCC_TB_CUSTOMER) { // XXX
    g_c_last_schema.add_column("c_id", mdb::Value::I32, true);
  }                                       // XXX
  verify(col_it == schema->end());

  // TODO (ycui) add a vector in tb_info_t to record used values for key.
  // ???
  uint64_t loc_num_records = tb_info->num_records;
  if (tb_info->tb_name == TPCC_TB_DISTRICT) {
    auto n_partition = Config::GetConfig()->GetNumPartition();
    loc_num_records *= n_partition;
  }
//  verify(tb_info->num_records >= num_foreign_row);
  verify(loc_num_records % num_foreign_row == 0 ||
         tb_info->num_records < num_foreign_row);
  num_self_primary = loc_num_records / num_foreign_row;
//  verify(num_self_primary > 0);
  Value key_value = value_get_zero(tb_info->columns[self_primary_col].type);
  Value max_key = value_get_n(tb_info->columns[self_primary_col].type,
                              num_self_primary);
  for (; key_value < max_key || num_self_primary == 0; ++key_value) {
    init_index(prim_foreign_index);
    InsertRow(tb_info, partition_id, key_value, schema, table_ptr, tbl_sec_ptr);
  }
  return 0;
}

} // namespace janus
