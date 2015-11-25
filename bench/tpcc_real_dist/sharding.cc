#include "__dep__.h"
#include "constants.h"
#include "config.h"
#include "dtxn.h"
#include "bench/tpcc_real_dist/sharding.h"
#include "bench/tpcc/piece.h"
#include "frame.h"
#include "scheduler.h"

namespace rococo {

void TPCCDSharding::temp2(tb_info_t *tb_info,
                          uint32_t &sid,
                          uint32_t &col_index,
                          std::vector<uint32_t> &bound_foreign_index,
                          uint64_t &num_self_primary,
                          map<uint32_t,
                              std::pair<uint32_t,
                                        uint32_t> > &prim_foreign_index,
                          std::vector<Value> &row_data,
                          Value &key_value,
                          const mdb::Schema *schema,
                          mdb::Table *const table_ptr,
                          mdb::SortedTable *tbl_sec_ptr) {

  bool record_key = true;
  int counter = 0;
  while (true) {

    row_data.clear();

    for (col_index = 0; col_index < tb_info->columns.size();
         col_index++) {
      if (tb_info->columns[col_index].is_primary) {
        if (prim_foreign_index.size() == 0) {
          if ((tb_info->tb_name != TPCC_TB_WAREHOUSE)
              && (tb_info->tb_name != TPCC_TB_ITEM)
              && (sid != site_from_key(key_value, tb_info)))
            break;
          row_data.push_back(key_value);

          if (tb_info->columns[col_index].values != NULL) {
            tb_info->columns[col_index].values->push_back(key_value);
          }
        } else if (tb_info->columns[col_index].foreign != NULL) {
          // primary key and foreign key

          Value v_buf;

          if ((tb_info->columns[col_index].foreign->name == "i_id")
              || (tb_info->columns[col_index].foreign->name ==
                  "w_id"))
            v_buf = Value((i32) prim_foreign_index[col_index].first);
          else
            v_buf = (*tb_info->columns[col_index].foreign->values)
            [prim_foreign_index[col_index].first];
          row_data.push_back(v_buf);
        } else { // primary key
          row_data.push_back(key_value);

          if ((tb_info->columns[col_index].values != NULL)
              && (tb_info->tb_name != TPCC_TB_DISTRICT)
              && record_key) {
            tb_info->columns[col_index].values->push_back(key_value);
            record_key = false;
          }
        }
      } else if (tb_info->columns[col_index].foreign != NULL) {
        bool use_key_value = false;
        int n;
        size_t fc_size =
            tb_info->columns[col_index].foreign->name.size();
        std::string last4 =
            tb_info->columns[col_index].foreign->name.substr(
                fc_size - 4,
                4);

        if (last4 == "i_id") {
          n = tb_infos_[std::string(TPCC_TB_ITEM)].num_records;
        }
        else if (last4 == "w_id") {
          n = tb_infos_[std::string(TPCC_TB_WAREHOUSE)].num_records;
        }
        else if (last4 == "c_id") {
          if (tb_info->columns[col_index].name ==
              "o_c_id")
            use_key_value = true;
          else
            n = tb_infos_[std::string(TPCC_TB_CUSTOMER)].num_records /
                tb_infos_[std::string(TPCC_TB_DISTRICT)].num_records;
        } else if (last4 == "d_id") {
          n = Config::GetConfig()->get_num_site() *
              tb_infos_[std::string(TPCC_TB_DISTRICT)].num_records /
              tb_infos_[std::string(TPCC_TB_WAREHOUSE)].num_records;
        } else {
          auto ftbl = tb_info->columns[col_index].foreign_tb;
          verify(ftbl);
          n = ftbl->num_records;
        }
        Value v_buf;

        if (use_key_value) {
          v_buf = key_value;
        } else {
          v_buf = value_get_n(tb_info->columns[col_index].type,
                              RandomGenerator::rand(0, n - 1));
        }
        row_data.push_back(v_buf);
      } else {
        Value
            v_buf = random_value(tb_info->columns[col_index].type);

        if (tb_info->columns[col_index].name ==
            "d_next_o_id")
          v_buf =
              Value((i32) (
                  tb_infos_[std::string(TPCC_TB_ORDER)].num_records /
                      tb_infos_[std::string(TPCC_TB_DISTRICT)].
                          num_records)); // XXX

        if (tb_info->columns[col_index].name ==
            "c_last")
          v_buf =
              Value(RandomGenerator::int2str_n(
                  key_value.get_i32() % 1000, 3));
        row_data.push_back(v_buf);
      }
    }

    if (col_index == tb_info->columns.size()) {
      if (tb_info->tb_name == TPCC_TB_STOCK) {
        int key_size;
        key_size = schema->key_columns_id().size();
        MultiValue mv(key_size);

        for (int i = 0; i < key_size;
             i++)
          mv[i] = row_data[schema->key_columns_id()[i]];

        if (sid == site_from_key(mv, tb_info)) {
          counter++;

          auto r = Frame().CreateRow(schema, row_data);
          table_ptr->insert(r);

          // log
          // std::string buf;
          // for (int i = 0; i < tb_info_ptr->columns.size(); i++)
          //
          //  buf.append(tb_info_ptr->columns[i].name).append(":").append(to_string(row_data[i])).append(";
          // ");
          // Log_debug("%s", buf.c_str());
        }
      } else if (tb_info->tb_name == TPCC_TB_DISTRICT) {
        int key_size;
        key_size = schema->key_columns_id().size();
        MultiValue mv(key_size);

        for (int i = 0; i < key_size;
             i++)
          mv[i] = row_data[schema->key_columns_id()[i]];

        if (sid == site_from_key(mv, tb_info)) {
          counter++;
          auto r = Frame().CreateRow(schema, row_data);
          table_ptr->insert(r);

          for (int i = 0; i < key_size;
               i++)
            tb_info->columns[schema->key_columns_id()[i]].
                values->push_back(mv[i]);

          // log
          // std::string buf;
          // for (int i = 0; i < tb_info_ptr->columns.size(); i++)
          //
          //  buf.append(tb_info_ptr->columns[i].name).append(":").append(to_string(row_data[i])).append(";
          // ");
          // Log_debug("%s", buf.c_str());
        }
      }
      else {
        counter++;
        auto r = Frame().CreateRow(schema, row_data);
        table_ptr->insert(r);

        // XXX order (o_d_id, o_w_id, o_c_id) --> maximum o_id
        if (tbl_sec_ptr) {
          rrr::i32 cur_o_id_buf = r->get_column("o_id").get_i32();
          const mdb::Schema *sch_buf = tbl_sec_ptr->schema();
          mdb::MultiBlob mb_buf(sch_buf->key_columns_id().size());
          mdb::Schema::iterator col_info_it = sch_buf->begin();
          size_t mb_i = 0;

          for (; col_info_it != sch_buf->end(); col_info_it++)
            if (col_info_it->indexed)
              mb_buf[mb_i++] = r->get_blob(
                  col_info_it->name);
          mdb::SortedTable::Cursor rs = tbl_sec_ptr->query(mb_buf);

          if (rs.has_next()) {
            mdb::Row *r_buf = rs.next();
            rrr::i32 o_id_buf = r_buf->get_column("o_id").get_i32();

            if (o_id_buf < cur_o_id_buf)
              r_buf->update("o_id",
                            cur_o_id_buf);
          }
          else {
            std::vector<Value> sec_row_data_buf;

            for (col_info_it = sch_buf->begin();
                 col_info_it != sch_buf->end();
                 col_info_it++) {
              sec_row_data_buf.push_back(r->get_column(col_info_it->name));
            }
            auto r = Frame().CreateRow(sch_buf, sec_row_data_buf);
            tbl_sec_ptr->insert(r);
          }
        }

        // XXX c_last secondary index
        if (tb_info->tb_name == TPCC_TB_CUSTOMER) {
          std::string c_last_buf = r->get_column(
              "c_last").get_str();
          rrr::i32 c_id_buf = r->get_column(
              "c_id").get_i32();
          size_t mb_size =
              g_c_last_schema.key_columns_id().size(), mb_i = 0;
          mdb::MultiBlob mb_buf(mb_size);
          mdb::Schema::iterator col_info_it = g_c_last_schema.begin();

          for (; col_info_it != g_c_last_schema.end(); col_info_it++) {
            mb_buf[mb_i++] = r->get_blob(col_info_it->name);
          }
          g_c_last2id.insert(std::make_pair(c_last_id_t(c_last_buf,
                                                        mb_buf,
                                                        &g_c_last_schema),
                                            c_id_buf));
        } // XXX

        // log
        // std::string buf;
        // for (int i = 0; i < tb_info_ptr->columns.size(); i++)
        //
        //  buf.append(tb_info_ptr->columns[i].name).append(":").append(to_string(row_data[i])).append(";
        // ");
        // Log_debug("%s", buf.c_str());
      }

      if ((num_self_primary == 0) &&
          (counter >= tb_info->num_records)) {
        num_self_primary = 1;
        break;
      }
    }

    if (num_self_primary == 0) {
      if (0 != index_increase(prim_foreign_index,
                              bound_foreign_index))
        verify(0);
    }
    else if (0 != index_increase(prim_foreign_index,
                                 bound_foreign_index))
      break;
  }
}

void TPCCDSharding::temp1(tb_info_t *tb_info,
                          uint32_t col_index,
                          mdb::Schema::iterator &col_it,
                          uint64_t &num_foreign_row,
                          std::vector<uint32_t> &bound_foreign_index,
                          uint64_t &num_self_primary,
                          uint32_t &self_primary_col,
                          bool &self_primary_col_find,
                          map<uint32_t,
                              std::pair<uint32_t,
                                        uint32_t> > &prim_foreign_index) {
  if (tb_info->columns[col_index].is_primary) {
    verify(col_it->indexed);

    if (tb_info->tb_name == TPCC_TB_CUSTOMER) { // XXX
      if (col_it->name != "c_id")
        g_c_last_schema.add_column(
            col_it->name.c_str(),
            col_it->type,
            true);
    } // XXX

    if (tb_info->columns[col_index].foreign_tb != NULL) {
      uint32_t tmp_int;
      uint32_t tmp_index_base;
      bool times_tmp_int = true;

      if (tb_info->columns[col_index].foreign->name == "i_id") {
        // refers to item.i_id, use all available i_id instead
        // of local i_id
        tmp_int = tb_info->columns[col_index].foreign_tb->num_records;
        tmp_index_base = tmp_int;

        if (tb_info->columns[col_index].values != NULL) {
          delete tb_info->columns[col_index].values;
          tb_info->columns[col_index].values = NULL;
        }
      } else if (tb_info->columns[col_index].foreign->name == "w_id") {
        // refers to warehouse.w_id, use all available w_id instead
        // of local w_id
        tmp_int = tb_info->columns[col_index].foreign_tb->num_records;
        tmp_index_base = tmp_int;

        if (tb_info->columns[col_index].values != NULL) {
          if (tb_info->tb_name == TPCC_TB_STOCK) {
            delete tb_info->columns[col_index].values;
            tb_info->columns[col_index].values = NULL;
          }
          else
            verify(tb_info->tb_name == TPCC_TB_DISTRICT);
        }
      } else {
        times_tmp_int = false;
        tmp_int = tb_info->columns[col_index].foreign_tb->num_records;

        if (num_foreign_row == 1) num_foreign_row *= tmp_int;
        column_t *foreign_column = tb_info->columns[col_index].foreign;
        verify(foreign_column->values);
        tmp_index_base = foreign_column->values->size();
        size_t foreign_col_name_size = foreign_column->name.size();

        if ((foreign_col_name_size >= 4)
            && ((foreign_column->name.substr(foreign_col_name_size - 4)
                == "w_id")
                || (foreign_column->name.substr(foreign_col_name_size -
                    4) == "d_id"))) {
          if (!bound_foreign_index.empty())
            verify(
                tmp_index_base ==
                    prim_foreign_index[bound_foreign_index[0]].second);
          bound_foreign_index.push_back(col_index);
        }

        if (tb_info->columns[col_index].values != NULL) {
          tb_info->columns[col_index].values->assign(
              foreign_column->values->begin(),
              foreign_column->values->end());
        }
      }
      verify(tmp_index_base > 0);
      prim_foreign_index[col_index] =
          std::pair<uint32_t, uint32_t>(0, tmp_index_base);

      if (times_tmp_int) num_foreign_row *= tmp_int;
    } else {
      // only one primary key can refer to no other table.
      verify(!self_primary_col_find);
      self_primary_col = col_index;
      self_primary_col_find = true;

      if ((tb_info->columns[col_index].name == "i_id")
          || (tb_info->columns[col_index].name == "w_id")) {
        if (tb_info->columns[col_index].values != NULL) {
          delete tb_info->columns[col_index].values;
          tb_info->columns[col_index].values = NULL;
        }
      }
    }
  }

}

void TPCCDSharding::PopulateTable(tb_info_t *tb_info, uint32_t sid) {
  int mode = Config::GetConfig()->get_mode();
  // find table and secondary table
  mdb::Table *const table_ptr = dtxn_mgr_->get_table(
      tb_info->tb_name);
  const mdb::Schema *schema = table_ptr->schema();
  mdb::SortedTable *tbl_sec_ptr = NULL;

  if (tb_info->tb_name == TPCC_TB_ORDER) {
    tbl_sec_ptr =
        (mdb::SortedTable *) dtxn_mgr_->get_table(
            TPCC_TB_ORDER_C_ID_SECONDARY);
  }
  verify(schema->columns_count() == tb_info->columns.size());

  uint64_t num_foreign_row = 1;
  uint64_t num_self_primary = 0;
  uint32_t self_primary_col = 0;
  bool self_primary_col_find = false;
  std::map<uint32_t,
           std::pair<uint32_t, uint32_t> > prim_foreign_index;
  std::vector<uint32_t> bound_foreign_index;
  mdb::Schema::iterator col_it = schema->begin();
  uint32_t col_index = 0;
  for (col_index = 0; col_index < tb_info->columns.size(); col_index++) {
    verify(col_it != schema->end());
    verify(tb_info->columns[col_index].name == col_it->name);
    verify(tb_info->columns[col_index].type == col_it->type);
    temp1(tb_info, col_index, col_it, num_foreign_row,
          bound_foreign_index, num_self_primary, self_primary_col,
          self_primary_col_find, prim_foreign_index);
    col_it++;
  }

  if (tb_info->tb_name == TPCC_TB_CUSTOMER) { // XXX
    g_c_last_schema.add_column("c_id", mdb::Value::I32, true);
  }                                       // XXX
  verify(col_it == schema->end());

  // TODO (ycui) add a vector in tb_info_t to record used values for key.
  uint64_t loc_num_records = (tb_info->tb_name == TPCC_TB_DISTRICT ?
                              Config::GetConfig()->get_num_site() : 1) *
      tb_info->num_records;
  verify(loc_num_records % num_foreign_row == 0
             || tb_info->num_records < num_foreign_row);
  num_self_primary = loc_num_records / num_foreign_row;
  Value key_value = value_get_zero(
      tb_info->columns[self_primary_col].type);
  Value max_key = value_get_n(tb_info->columns[self_primary_col].type,
                              num_self_primary);
  std::vector<Value> row_data;
  row_data.reserve(tb_info->columns.size());

  // log
  // Log_debug("begin:%s", tb_it->first.c_str());
  for (; key_value < max_key || num_self_primary == 0; ++key_value) {
    init_index(prim_foreign_index);
    temp2(tb_info, sid, col_index, bound_foreign_index,
          num_self_primary, prim_foreign_index, row_data,
          key_value, schema, table_ptr, tbl_sec_ptr);
  }
  // log
  // Log_debug("end:%s", tb_it->first.c_str());
  tb_info->populated[sid] = true;
}

int TPCCDSharding::PopulateTable(uint32_t sid) {
  uint32_t number2populate = tb_infos_.size();
  verify(number2populate > 0);

  while (number2populate > 0) {
    uint32_t pre_num2populate = number2populate;
//    std::map<std::string, tb_info_t>::iterator tb_it = tb_info_.begin();

    for (auto tb_it = tb_infos_.begin(); tb_it != tb_infos_.end(); tb_it++) {
      tb_info_t *tb_info = &(tb_it->second);
      verify(tb_it->first == tb_info->tb_name);

      // TODO is this unnecessary?
      auto it = tb_info->populated.find(sid);
      if (it == tb_info->populated.end()) {
        tb_info->populated[sid] = false;
      }

      if (!tb_info->populated[sid] && Ready2Populate(tb_info)) {
        PopulateTable(tb_info, sid);
        // finish populate one table
        number2populate--;
      }
    }
    verify(pre_num2populate > number2populate);
  }

  release_foreign_values();

  return 0;
}
} // namespace rococo
