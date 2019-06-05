#include "sharding.h"
#include "workload.h"
#include "deptran/frame.h"
#include "deptran/scheduler.h"

namespace janus {

int TpccSharding::PopulateTable(tb_info_t *tb_info_ptr, parid_t par_id) {

  mdb::Table *const table_ptr = tx_sched_->get_table(tb_info_ptr->tb_name);
  const mdb::Schema *schema = table_ptr->schema();
  verify(schema->columns_count() == tb_info_ptr->columns.size());


  uint32_t col_index = 0;
  auto n_partition = Config::GetConfig()->GetNumPartition();
  if (tb_info_ptr->tb_name == TPCC_TB_WAREHOUSE) { // warehouse table
    Value key_value, max_key;
    mdb::Schema::iterator col_it = schema->begin();
    for (col_index = 0; col_index < tb_info_ptr->columns.size(); col_index++) {
      verify(col_it != schema->end());
      verify(tb_info_ptr->columns[col_index].name == col_it->name);
      verify(tb_info_ptr->columns[col_index].type == col_it->type);
      if (tb_info_ptr->columns[col_index].is_primary) {
        verify(col_it->indexed);
        key_value = value_get_zero(tb_info_ptr->columns[col_index].type);
        max_key = value_get_n(tb_info_ptr->columns[col_index].type,
                              tb_info_ptr->num_records * n_partition);
      }
      col_it++;
    }
    verify(col_it == schema->end());
    std::vector<Value> row_data;
    for (; key_value < max_key; ++key_value) {
      row_data.clear();
      for (col_index = 0; col_index < tb_info_ptr->columns.size();
           col_index++) {
        if (tb_info_ptr->columns[col_index].is_primary) {
          if (par_id != PartitionFromKey(key_value, tb_info_ptr))
            break;
          if (tb_info_ptr->columns[col_index].values != NULL)
            tb_info_ptr->columns[col_index].values->push_back(key_value);
          row_data.push_back(key_value);
          //Log_debug("%s (primary): %s", tb_info_ptr->columns[col_index].name.c_str(), to_string(key_value).c_str());
          //std::cerr << tb_info_ptr->columns[col_index].name << "(primary):" << row_data.back() << "; ";
        }
        else if (tb_info_ptr->columns[col_index].foreign != NULL) {
          // TODO (ycui) use RandomGenerator
          Log_fatal("Table %s shouldn't have a foreign key!",
                    TPCC_TB_WAREHOUSE);
          verify(0);
        }
        else {
          // TODO (ycui) use RandomGenerator
          Value v_buf = random_value(tb_info_ptr->columns[col_index].type);
          row_data.push_back(v_buf);
        }
      }
      if (col_index == tb_info_ptr->columns.size()) {
        auto row = frame_->CreateRow(schema, row_data);
        table_ptr->insert(row);
      }
    }
  } else { // non warehouse tables
    unsigned long long int num_foreign_row = 1;
    unsigned long long int num_self_primary = 0;
    unsigned int self_primary_col = 0;
    bool self_primary_col_find = false;
    std::map<unsigned int, std::pair<unsigned int, unsigned int> >
        prim_foreign_index;
    mdb::Schema::iterator col_it = schema->begin();

    mdb::SortedTable *tbl_sec_ptr = NULL;
    if (tb_info_ptr->tb_name == TPCC_TB_ORDER)
      tbl_sec_ptr =(mdb::SortedTable *) tx_sched_->get_table(
          TPCC_TB_ORDER_C_ID_SECONDARY);
    for (col_index = 0; col_index < tb_info_ptr->columns.size(); col_index++) {
      verify(col_it != schema->end());
      verify(tb_info_ptr->columns[col_index].name == col_it->name);
      verify(tb_info_ptr->columns[col_index].type == col_it->type);
      if (tb_info_ptr->columns[col_index].is_primary) {
        verify(col_it->indexed);
        if (tb_info_ptr->tb_name == TPCC_TB_CUSTOMER) { //XXX
          if (col_it->name != "c_id")
            g_c_last_schema.add_column(col_it->name.c_str(),
                                       col_it->type,
                                       true);
        } //XXX
        if (tb_info_ptr->columns[col_index].foreign_tb != NULL) {
          unsigned int tmp_int;
          if (tb_info_ptr->columns[col_index].foreign->name
              == "i_id") { // refers to item.i_id, use all available i_id instead of local i_id
            tmp_int = tb_info_ptr->columns[col_index].foreign_tb->num_records;
            if (tb_info_ptr->columns[col_index].values != NULL) {
              tb_info_ptr->columns[col_index].values->resize(tmp_int);
              for (i32 i = 0; i < tmp_int; i++)
                (*tb_info_ptr->columns[col_index].values)[i] = Value(i);
            }
          }
          else {
            column_t *foreign_column = tb_info_ptr->columns[col_index].foreign;
            tmp_int = foreign_column->values->size();
            if (tb_info_ptr->columns[col_index].values != NULL) {
              tb_info_ptr->columns[col_index].values->assign(foreign_column->values->begin(),
                                                             foreign_column->values->end());
            }
          }
          verify(tmp_int > 0);
          prim_foreign_index[col_index] =
              std::pair<unsigned int, unsigned int>(0, tmp_int);
          num_foreign_row *= tmp_int;
        }
        else {
          // only one primary key can refer to no other table.
          verify(!self_primary_col_find);
          self_primary_col = col_index;
          self_primary_col_find = true;
        }
      }
      col_it++;
    }
    if (tb_info_ptr->tb_name == TPCC_TB_CUSTOMER) { //XXX
      g_c_last_schema.add_column("c_id", mdb::Value::I32, true);
    } //XXX
    // TODO (ycui) add a vector in tb_info_t to record used values for key.
    verify(tb_info_ptr->num_records % num_foreign_row == 0
               || tb_info_ptr->num_records < num_foreign_row);
    //Log_debug("foreign row: %llu, this row: %llu", num_foreign_row, tb_info_ptr->num_records);
    num_self_primary = tb_info_ptr->num_records / num_foreign_row;
    Value
        key_value = value_get_zero(tb_info_ptr->columns[self_primary_col].type);
    Value max_key = value_get_n(tb_info_ptr->columns[self_primary_col].type,
                                num_self_primary);
    std::vector<Value> row_data;
    //Log_debug("Begin primary key: %s, Max primary key: %s", to_string(key_value).c_str(), to_string(max_key).c_str());


    for (; key_value < max_key || num_self_primary == 0; ++key_value) {

      bool record_key = true;
      init_index(prim_foreign_index);
      int counter = 0;
      while (true) {
        row_data.clear();
        for (col_index = 0; col_index < tb_info_ptr->columns.size();
             col_index++) {
          if (tb_info_ptr->columns[col_index].is_primary) {
            if (prim_foreign_index.size() == 0) {
              if (par_id != PartitionFromKey(key_value, tb_info_ptr))
                break;
              row_data.push_back(key_value);
              if (tb_info_ptr->columns[col_index].values != NULL) {
                tb_info_ptr->columns[col_index].values->push_back(key_value);
              }
              //Log_debug("%s (primary): %s", tb_info_ptr->columns[col_index].name.c_str(), to_string(key_value).c_str());
            }
              // primary key and foreign key
            else if (tb_info_ptr->columns[col_index].foreign != NULL) {
              Value v_buf;
              if (tb_info_ptr->columns[col_index].foreign->name == "i_id")
                v_buf = Value((i32) prim_foreign_index[col_index].first);
              else
                v_buf =
                    (*tb_info_ptr->columns[col_index].foreign->values)[prim_foreign_index[col_index].first];
              //Log_debug("%s (primary, foreign): %s", tb_info_ptr->columns[col_index].name.c_str(), to_string(v_buf).c_str());
              row_data.push_back(v_buf);
            }
            else { // primary key
              row_data.push_back(key_value);
              if (tb_info_ptr->columns[col_index].values != NULL
                  && record_key) {
                tb_info_ptr->columns[col_index].values->push_back(key_value);
                record_key = false;
              }
              //Log_debug("%s (primary): %s", tb_info_ptr->columns[col_index].name.c_str(), to_string(key_value).c_str());
            }
          }
          else if (tb_info_ptr->columns[col_index].foreign != NULL) {
            bool use_key_value = false;
            int n;
            size_t
                fc_size = tb_info_ptr->columns[col_index].foreign->name.size();
            std::string last4 =
                tb_info_ptr->columns[col_index].foreign->name.substr(
                    fc_size - 4, 4);
            if (last4 == "i_id") {
              n = tb_infos_[std::string(TPCC_TB_ITEM)].num_records;
            }
            else if (last4 == "w_id") {
              n = tb_infos_[std::string(TPCC_TB_WAREHOUSE)].num_records
                  * n_partition;
            }
            else if (last4 == "c_id") {
              if (tb_info_ptr->columns[col_index].name == "o_c_id")
                use_key_value = true;
              else
                n = tb_infos_[std::string(TPCC_TB_CUSTOMER)].num_records
                    / tb_infos_[std::string(TPCC_TB_DISTRICT)].num_records;
            }
            else if (last4 == "d_id") {
              n = tb_infos_[std::string(TPCC_TB_DISTRICT)].num_records
                  / tb_infos_[std::string(TPCC_TB_WAREHOUSE)].num_records;
            }
            else {
              n = tb_info_ptr->columns[col_index].foreign_tb->num_records;
            }
            Value v_buf;
            if (use_key_value)
              v_buf = key_value;
            else
              v_buf = value_get_n(tb_info_ptr->columns[col_index].type,
                                  RandomGenerator::rand(0, n - 1));
            row_data.push_back(v_buf);
            //Log_debug("%s (foreign): %s", tb_info_ptr->columns[col_index].name.c_str(), to_string(v_buf).c_str());
          }
          else {
            Value v_buf = random_value(tb_info_ptr->columns[col_index].type);
            if (tb_info_ptr->columns[col_index].name == "d_next_o_id")
              v_buf =
                  Value((i32) (tb_infos_[std::string(TPCC_TB_ORDER)].num_records
                      / tb_infos_[std::string(TPCC_TB_DISTRICT)].num_records)
                  ); //XXX
            if (tb_info_ptr->columns[col_index].name == "c_last")
              v_buf =
                  Value(RandomGenerator::int2str_n(key_value.get_i32() % 1000,
                                                   3));
            row_data.push_back(v_buf);
            //Log_debug("%s: %s", tb_info_ptr->columns[col_index].name.c_str(), to_string(v_buf).c_str());
          }
        }
        if (col_index == tb_info_ptr->columns.size()) {
          // log
          //std::string buf;
          //for (int i = 0; i < tb_info_ptr->columns.size(); i++)
          //    buf.append(tb_info_ptr->columns[i].name).append(":").append(to_string(row_data[i])).append("; ");
          //rrr::Log::info("%s", buf.c_str());

          mdb::Row *r = frame_->CreateRow(schema, row_data);
          table_ptr->insert(r);
          if (tb_info_ptr->tb_name == TPCC_TB_STOCK && par_id == 1 ) {
            auto item_id = row_data[0].get_i32();
            if (item_id == 9999)
              Log_debug("debug point here. %d", item_id);
          }

          //
          if (tbl_sec_ptr) {
            rrr::i32 cur_o_id_buf = r->get_column("o_id").get_i32();
            const mdb::Schema *sch_buf = tbl_sec_ptr->schema();
            mdb::MultiBlob mb_buf(sch_buf->key_columns_id().size());
            mdb::Schema::iterator col_info_it = sch_buf->begin();
            size_t mb_i = 0;
            for (; col_info_it != sch_buf->end(); col_info_it++)
              if (col_info_it->indexed)
                mb_buf[mb_i++] = r->get_blob(col_info_it->name);
            mdb::SortedTable::Cursor rs = tbl_sec_ptr->query(mb_buf);
            if (rs.has_next()) {
              mdb::Row *r_buf = rs.next();
              rrr::i32 o_id_buf = r_buf->get_column("o_id").get_i32();
              if (o_id_buf < cur_o_id_buf)
                r_buf->update("o_id", cur_o_id_buf);
            }
            else {
              std::vector<Value> sec_row_data_buf;
              for (col_info_it = sch_buf->begin();
                   col_info_it != sch_buf->end(); col_info_it++)
                sec_row_data_buf.push_back(r->get_column(col_info_it->name));
              mdb::Row *r_buf = frame_->CreateRow(sch_buf, sec_row_data_buf);
              tbl_sec_ptr->insert(r_buf);
            }
          }


          //XXX c_last secondary index
          if (tb_info_ptr->tb_name == TPCC_TB_CUSTOMER) {
            std::string c_last_buf = r->get_column("c_last").get_str();
            rrr::i32 c_id_buf = r->get_column("c_id").get_i32();
            size_t mb_size = g_c_last_schema.key_columns_id().size(), mb_i = 0;
            mdb::MultiBlob mb_buf(mb_size);
            mdb::Schema::iterator col_info_it = g_c_last_schema.begin();
            for (; col_info_it != g_c_last_schema.end(); col_info_it++) {
              mb_buf[mb_i++] = r->get_blob(col_info_it->name);
            }
            g_c_last2id.insert(std::make_pair(c_last_id_t(c_last_buf,
                                                          mb_buf,
                                                          &g_c_last_schema),
                                              c_id_buf));
          } //XXX

          //Log_debug("Row inserted");
          counter++;
          if (num_self_primary == 0 && counter >= tb_info_ptr->num_records) {
            num_self_primary = 1;
            break;
          }
        }
        if (num_self_primary == 0) {
          if (0 != index_increase(prim_foreign_index))
            verify(0);
        }
        else if (0 != index_increase(prim_foreign_index))
          break;
      }
    }
  }
  return 0;
}

int TpccSharding::PopulateTables(parid_t par_id) {
  return Sharding::PopulateTables(par_id);
}

} // namespace janus
