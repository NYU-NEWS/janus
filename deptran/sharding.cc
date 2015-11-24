#include "constants.h"
#include "sharding.h"

// for tpca benchmark
#include "bench/tpca/piece.h"
#include "bench/tpca/chopper.h"

// tpcc benchmark
#include "bench/tpcc/piece.h"
#include "bench/tpcc/chopper.h"

// tpcc dist partition benchmark
#include "bench/tpcc_dist/piece.h"
#include "bench/tpcc_dist/chopper.h"

// tpcc real dist partition benchmark
#include "bench/tpcc_real_dist/piece.h"
#include "bench/tpcc_real_dist/chopper.h"

// rw benchmark
#include "bench/rw_benchmark/piece.h"
#include "bench/rw_benchmark/chopper.h"

// micro bench
#include "bench/micro/piece.h"
#include "bench/micro/chopper.h"

namespace rococo {
//Sharding *Sharding::sharding_s = NULL;


Sharding::Sharding() { }

Sharding::Sharding(const Sharding &sharding)
    : tb_infos_(sharding.tb_infos_), dist2sid_(sharding.dist2sid_),
      stock2sid_(sharding.stock2sid_) {
  BuildTableInfoPtr();
}

Sharding::~Sharding() {
  std::map<std::string, tb_info_t>::iterator it;

  for (it = tb_infos_.begin(); it != tb_infos_.end(); it++)
    if (it->second.site_id) free(it->second.site_id);
}

void Sharding::BuildTableInfoPtr() {
  verify(tb_infos_.size() > 0);
  for (auto tbl_it = tb_infos_.begin(); tbl_it != tb_infos_.end(); tbl_it++ ) {
    auto &tbl = tbl_it->second;
    auto &columns = tbl.columns;
    verify(columns.size() > 0);
    for (auto col_it = columns.begin(); col_it != columns.end(); col_it++) {
      auto &col = *col_it;
      if (col.is_foreign) {
        verify(!col.foreign_col_name.empty());
        auto ftb_it = tb_infos_.find(col.foreign_tbl_name);
        verify(ftb_it != tb_infos_.end());
        auto &ftbl = ftb_it->second;
        col.foreign_tb = &ftbl;
        auto fcol_it = ftbl.columns.begin();
        for (; fcol_it != ftbl.columns.end(); fcol_it++) {
          if (fcol_it->name == col.foreign_col_name) {
            verify(fcol_it->type == col.type);
            if (fcol_it->values == NULL) {
              fcol_it->values = new std::vector<Value>();
            }
            col.foreign = &(*fcol_it);
            break;
          }
        }
        verify(fcol_it != ftbl.columns.end());
      }
    }

  }

}

uint32_t Sharding::site_from_key(const MultiValue &key,
                                 const tb_info_t *tb_info) {
  const MultiValue &key_buf =
      Config::GetConfig()->get_benchmark() != TPCC_REAL_DIST_PART ?
      key :
      (tb_info->tb_name == TPCC_TB_STOCK
           || tb_info->tb_name == TPCC_TB_ITEM ?
       stock_mapping(key) :
       (tb_info->tb_name != TPCC_TB_HISTORY ?
        dist_mapping(key) : key));

  //    if (tb_info->tb_name == TPCC_TB_STOCK
  //     || tb_info->tb_name == TPCC_TB_ITEM)
  //        key_buf = sharding_s->stock_mapping(key);
  //    else if (tb_info->tb_name != TPCC_TB_HISTORY)
  //        key_buf = sharding_s->dist_mapping(key);
  //    else
  //        key_buf = key;
  // else
  //    key_buf = key;
  uint32_t ret;

  switch (tb_info->sharding_method) {
    case MODULUS:
      ret = modulus(key_buf, tb_info->num_site, tb_info->site_id);
      break;

    case INT_MODULUS:
      ret = int_modulus(key_buf, tb_info->num_site, tb_info->site_id);
      break;

    default:
      ret = modulus(key_buf, tb_info->num_site, tb_info->site_id);
      break;
  }
  return ret;
}

uint32_t Sharding::modulus(const MultiValue &key,
                           uint32_t num_site,
                           const uint32_t *site_id) {
  uint32_t index = 0;
  long long int buf;
  int i = 0;

  while (i < key.size()) {
    switch (key[i].get_kind()) {
      case Value::I32:
        buf = key[i].get_i32() % num_site;
        index += buf > 0 ? (uint32_t) buf : (uint32_t) (-buf);
        break;

      case Value::I64:
        buf = key[i].get_i64() % num_site;
        index += buf > 0 ? (uint32_t) buf : (uint32_t) (-buf);
        break;

      case Value::DOUBLE:
        buf = ((long long int) floor(key[i].get_double())) % num_site;
        index += buf > 0 ? (uint32_t) buf : (uint32_t) (-buf);
        break;

      case Value::STR: {
        uint32_t sum = 0;
        const std::string &str_buf = key[i].get_str();
        size_t i = 0;

        for (; i < str_buf.size(); i++) sum += (uint32_t) str_buf[i];
        index += sum % num_site;
      }

      case Value::UNKNOWN:
        Log_error("NOT IMPLEMENTED");
        verify(0);
        break;

      default:
        Log_error("UNKWOWN VALUE TYPE");
        verify(0);
        break;
    }
    i++;
    index %= num_site;
  }
  return site_id[index % num_site];
}

uint32_t Sharding::int_modulus(const MultiValue &key,
                               uint32_t num_site,
                               const uint32_t *site_id) {
  uint32_t index = 0;
  long long int buf;
  int i = 0;

  while (i < key.size()) {
    switch (key[i].get_kind()) {
      case Value::I32:
        buf = key[i].get_i32() % num_site;
        index += buf > 0 ? (uint32_t) buf : (uint32_t) (-buf);
        break;

      case Value::I64:
        buf = key[i].get_i64() % num_site;
        index += buf > 0 ? (uint32_t) buf : (uint32_t) (-buf);
        break;

      case Value::DOUBLE:
        buf = ((long long int) floor(key[i].get_double())) % num_site;
        index += buf > 0 ? (uint32_t) buf : (uint32_t) (-buf);
        break;

      case Value::STR: {
        uint32_t sum = 0;
        uint32_t mod = 1;
        const std::string &str_buf = key[i].get_str();
        std::string::const_reverse_iterator rit = str_buf.rbegin();

        for (; rit != str_buf.rend(); rit++) {
          sum += mod * (uint32_t) (*rit);
          sum %= num_site;
          mod *= 127;
          mod %= num_site;
        }
        index += sum % num_site;
      }

      case Value::UNKNOWN:
        Log_error("NOT IMPLEMENTED");
        verify(0);
        break;

      default:
        Log_error("ERROR");
        verify(0);
        break;
    }

    i++;
    index %= num_site;
  }
  return site_id[index % num_site];
}

void Sharding::insert_dist_mapping(const MultiValue &mv) {
  MultiValue v(Value((i32) dist2sid_.size()));

  dist2sid_.insert(std::pair<MultiValue, MultiValue>(mv, v));
}

MultiValue &Sharding::dist_mapping(const MultiValue &mv) {
  return dist2sid_[mv];
}

void Sharding::insert_stock_mapping(const MultiValue &mv) {
  MultiValue v(Value((i32) stock2sid_.size()));

  stock2sid_.insert(std::pair<MultiValue, MultiValue>(mv, v));
}

MultiValue &Sharding::stock_mapping(const MultiValue &mv) {
  return stock2sid_[mv];
}

int Sharding::get_site_id_from_tb(const std::string &tb_name,
                                  const MultiValue &key,
                                  uint32_t &site_id) {
  std::map<std::string, tb_info_t>::iterator it = tb_infos_.find(tb_name);

  if (it == tb_infos_.end()) return -1;

  if ((it->second.num_site == 0) || (it->second.site_id == NULL)) return -2;

  site_id = site_from_key(key, &(it->second));
  return 0;
}

int Sharding::get_site_id_from_tb(const std::string &tb_name,
                                  std::vector<uint32_t> &site_id) {
  std::map<std::string, tb_info_t>::iterator it = tb_infos_.find(tb_name);

  if (it == tb_infos_.end()) return -1;

  if ((it->second.num_site == 0) || (it->second.site_id == NULL)) return -2;

  site_id.clear();
  uint32_t i = 0;

  for (; i < it->second.num_site; i++) site_id.push_back(it->second.site_id[i]);
  return 0;
}

//int Sharding::get_site_id(const char *tb_name,
//                          const Value &key,
//                          uint32_t &site_id) {
//  if (!sharding_s) return -1;
//
//  if (tb_name == NULL) return -2;
//
//  std::string tb_name_str(tb_name);
//  return sharding_s->get_site_id_from_tb(tb_name_str, MultiValue(key), site_id);
//}
//
//int Sharding::get_site_id(const std::string &tb_name,
//                          const Value &key,
//                          uint32_t &site_id) {
//  if (!sharding_s) return -1;
//
//  return sharding_s->get_site_id_from_tb(tb_name, MultiValue(key), site_id);
//}
//
//int Sharding::get_site_id(const char *tb_name,
//                          const MultiValue &key,
//                          uint32_t &site_id) {
//  if (!sharding_s) return -1;
//
//  if (tb_name == NULL) return -2;
//
//  std::string tb_name_str(tb_name);
//  return sharding_s->get_site_id_from_tb(tb_name_str, key, site_id);
//}
//
//int Sharding::get_site_id(const std::string &tb_name,
//                          const MultiValue &key,
//                          uint32_t &site_id) {
//  if (!sharding_s) return -1;
//
//  return sharding_s->get_site_id_from_tb(tb_name, key, site_id);
//}
//
//int Sharding::get_site_id(const char *tb_name,
//                          std::vector<uint32_t> &site_id) {
//  if (!sharding_s) return -1;
//
//  if (tb_name == NULL) return -2;
//
//  std::string tb_name_str(tb_name);
//  return sharding_s->get_site_id_from_tb(tb_name_str, site_id);
//}
//
//int Sharding::get_site_id(const std::string &tb_name,
//                          std::vector<uint32_t> &site_id) {
//  if (!sharding_s) return -1;
//
//  return sharding_s->get_site_id_from_tb(tb_name, site_id);
//}

//int Sharding::init_schema(const char *tb_name,
//                          mdb::Schema *schema,
//                          mdb::symbol_t *symbol) {
//  return init_schema(std::string(tb_name), schema, symbol);
//}
//
int Sharding::init_schema(const std::string &tb_name,
                          mdb::Schema *schema,
                          mdb::symbol_t *symbol) {
  auto &tb_infos = tb_infos_;
  std::map<std::string, tb_info_t>::iterator it;

  it = tb_infos.find(tb_name);

  if (it == tb_infos.end()) return -1;
  auto &tb_info = it->second;
  std::vector<column_t>::iterator column_it = tb_info.columns.begin();

  for (; column_it != tb_info.columns.end(); column_it++) {
    schema->add_column(
        column_it->name.c_str(), column_it->type, column_it->is_primary);
  }
  *symbol = tb_info.symbol;
  return schema->columns_count();
}
//
int Sharding::get_table_names(uint32_t sid,
                              std::vector<std::string> &tables) {
  tables.clear();
  std::map<std::string, tb_info_t>::iterator it = tb_infos_.begin();
  uint32_t i;

  for (; it != tb_infos_.end(); it++) {
    auto &tb_info = it->second;

    for (i = 0; i < tb_info.num_site; i++) {
      if (tb_info.site_id[i] == sid) {
        tables.push_back(it->first);
        break;
      }
    }
  }
  return tables.size();
}

bool Sharding::Ready2Populate(tb_info_t *tb_info) {

  auto &columns = tb_info->columns;

  for (auto c_it = columns.begin(); c_it != columns.end(); c_it++)
    if ((c_it->foreign != NULL) && (c_it->foreign->values != NULL) &&
        (c_it->foreign->values->size() == 0))
      // have foreign table
      // foreign table has some mysterious values
      // those values have not been put in
      return false;

  return true;
}

// TODO this should be moved to per benchmark class
int Sharding::PopulateTable(uint32_t sid) {

  verify(0);
  switch (Config::GetConfig()->get_benchmark()) {
    case TPCC:
//      return sharding_s->do_tpcc_populate_table(sid);
    case TPCC_DIST_PART:
//      return sharding_s->do_tpcc_dist_partition_populate_table(table_map, sid);
//    case TPCC_REAL_DIST_PART:
//      return sharding_s->do_tpcc_real_dist_partition_populate_table(table_map,
//                                                                    sid);
    case TPCA:
    default:
//      return sharding_s->do_populate_table(table_map, sid);
      verify(0);
  }
}


//int Sharding::do_tpcc_dist_partition_populate_table(
//    const std::vector<std::string> &table_names,
//    uint32_t sid) {
//  int mode = Config::GetConfig()->get_mode();
//  uint32_t number2populate = tb_infos_.size();
//
//  while (number2populate > 0) {
//    uint32_t pre_num2populate = number2populate;
//    std::map<std::string, tb_info_t>::iterator tb_it = tb_infos_.begin();
//
//    for (; tb_it != tb_infos_.end(); tb_it++) {
//      if (!tb_it->second.populated[sid] && Ready2Populate(&(tb_it->second))) {
//        tb_info_t *tb_info_ptr = &(tb_it->second);
//        mdb::Table *const table_ptr = DTxnMgr::GetTxnMgr(sid)->get_table(
//            tb_it->first);
//        const mdb::Schema *schema = table_ptr->schema();
//        verify(schema->columns_count() == tb_info_ptr->columns.size());
//
//        uint64_t num_foreign_row = 1;
//        uint64_t num_self_primary = 0;
//        uint32_t self_primary_col = 0;
//        bool self_primary_col_find = false;
//        std::map<uint32_t,
//                 std::pair<uint32_t, uint32_t> > prim_foreign_index;
//        std::vector<uint32_t> bound_foreign_index;
//        mdb::Schema::iterator col_it = schema->begin();
//        uint32_t col_index = 0;
//
//        for (col_index = 0; col_index < tb_info_ptr->columns.size();
//             col_index++) {
//          verify(col_it != schema->end());
//          verify(tb_info_ptr->columns[col_index].name == col_it->name);
//          verify(tb_info_ptr->columns[col_index].type == col_it->type);
//
//          if (tb_info_ptr->columns[col_index].is_primary) {
//            verify(col_it->indexed);
//
//            if (tb_it->first == TPCC_TB_CUSTOMER) { // XXX
//              if (col_it->name != "c_id")
//                g_c_last_schema.add_column(
//                    col_it->name.c_str(),
//                    col_it->type,
//                    true);
//            } // XXX
//
//            if (tb_info_ptr->columns[col_index].foreign_tb != NULL) {
//              uint32_t tmp_int;
//              uint32_t tmp_index_base;
//              bool times_tmp_int = true;
//
//              if (tb_info_ptr->columns[col_index].foreign->name == "i_id") {
//                // refers
//                // to
//                // item.i_id,
//                // use
//                // all
//                // available
//                // i_id
//                // instead
//                // of
//                // local
//                // i_id
//                tmp_int =
//                    tb_info_ptr->columns[col_index].foreign_tb->num_records;
//                tmp_index_base = tmp_int;
//
//                if (tb_info_ptr->columns[col_index].values != NULL) {
//                  delete tb_info_ptr->columns[col_index].values;
//                  tb_info_ptr->columns[col_index].values = NULL;
//                }
//              }
//              else if (tb_info_ptr->columns[col_index].foreign->name
//                  == "w_id") {
//                // refers to warehouse.w_id, use all available w_id
//                // instead of local w_id
//                tmp_int =
//                    tb_info_ptr->columns[col_index].foreign_tb->num_records;
//                tmp_index_base = tmp_int * Config::GetConfig()->get_num_site();
//
//                if (tb_info_ptr->columns[col_index].values != NULL) {
//                  if (tb_it->first == TPCC_TB_STOCK) {
//                    delete tb_info_ptr->columns[col_index].values;
//                    tb_info_ptr->columns[col_index].values = NULL;
//                  }
//                  else
//                    verify(tb_it->first == TPCC_TB_DISTRICT);
//                }
//              }
//              else {
//                times_tmp_int = false;
//                tmp_int =
//                    tb_info_ptr->columns[col_index].foreign_tb->num_records;
//
//                if (num_foreign_row == 1) num_foreign_row *= tmp_int;
//                column_t *foreign_column =
//                    tb_info_ptr->columns[col_index].foreign;
//                tmp_index_base = foreign_column->values->size();
//                size_t foreign_col_name_size = foreign_column->name.size();
//
//                if ((foreign_col_name_size >= 4)
//                    && ((foreign_column->name.substr(foreign_col_name_size - 4)
//                        ==
//                            "w_id")
//                        || (foreign_column->name.substr(foreign_col_name_size -
//                            4) == "d_id"))) {
//                  if (!bound_foreign_index.empty())
//                    verify(
//                        tmp_index_base ==
//                            prim_foreign_index[bound_foreign_index[0]].second);
//                  bound_foreign_index.push_back(col_index);
//                }
//
//                if (tb_info_ptr->columns[col_index].values != NULL) {
//                  tb_info_ptr->columns[col_index].values->assign(
//                      foreign_column->values->begin(),
//                      foreign_column->values->end());
//                }
//              }
//              verify(tmp_index_base > 0);
//              prim_foreign_index[col_index] =
//                  std::pair<uint32_t, uint32_t>(0, tmp_index_base);
//
//              if (times_tmp_int) num_foreign_row *= tmp_int;
//            }
//            else {
//              // only one primary key can refer to no other table.
//              verify(!self_primary_col_find);
//              self_primary_col = col_index;
//              self_primary_col_find = true;
//
//              if ((tb_info_ptr->columns[col_index].name == "i_id")
//                  || (tb_info_ptr->columns[col_index].name == "w_id")) {
//                if (tb_info_ptr->columns[col_index].values != NULL) {
//                  delete tb_info_ptr->columns[col_index].values;
//                  tb_info_ptr->columns[col_index].values = NULL;
//                }
//              }
//            }
//          }
//          col_it++;
//        }
//
//        if (tb_it->first == TPCC_TB_CUSTOMER) { // XXX
//          g_c_last_schema.add_column("c_id", mdb::Value::I32, true);
//        }                                       // XXX
//        verify(col_it == schema->end());
//
//        // TODO (ycui) add a vector in tb_info_t to record used values for key.
//        verify(tb_info_ptr->num_records % num_foreign_row == 0
//                   || tb_info_ptr->num_records < num_foreign_row);
//
//        // Log_debug("foreign row: %llu, this row: %llu", num_foreign_row,
//        // tb_info_ptr->num_records);
//        num_self_primary = tb_info_ptr->num_records / num_foreign_row;
//        Value key_value = value_get_zero(
//            tb_info_ptr->columns[self_primary_col].type);
//        Value max_key = value_get_n(tb_info_ptr->columns[self_primary_col].type,
//                                    num_self_primary *
//                                        (tb_it->first ==
//                                            TPCC_TB_WAREHOUSE
//                                         ? Config::GetConfig()->
//                                                get_num_site() : 1));
//        std::vector<Value> row_data;
//        row_data.reserve(tb_info_ptr->columns.size());
//
//        // log
//        // Log_debug("begin:%s", tb_it->first.c_str());
//        for (; key_value < max_key || num_self_primary == 0; ++key_value) {
//          bool record_key = true;
//          init_index(prim_foreign_index);
//          int counter = 0;
//
//          while (true) {
//            row_data.clear();
//
//            for (col_index = 0; col_index < tb_info_ptr->columns.size();
//                 col_index++) {
//              if (tb_info_ptr->columns[col_index].is_primary) {
//                if (prim_foreign_index.size() == 0) {
//                  if (sid != site_from_key(key_value, tb_info_ptr)) break;
//                  row_data.push_back(key_value);
//
//                  if (tb_info_ptr->columns[col_index].values != NULL) {
//                    tb_info_ptr->columns[col_index].values->push_back(key_value);
//                  }
//                }
//
//                  // primary key and foreign key
//                else if (tb_info_ptr->columns[col_index].foreign != NULL) {
//                  Value v_buf;
//
//                  if ((tb_info_ptr->columns[col_index].foreign->name == "i_id")
//                      || (tb_info_ptr->columns[col_index].foreign->name ==
//                          "w_id"))
//                    v_buf = Value(
//                        (i32) prim_foreign_index[col_index].first);
//                  else
//                    v_buf =
//                        (*tb_info_ptr->columns[col_index].foreign->values)[
//                            prim_foreign_index
//                            [
//                                col_index].first];
//                  row_data.push_back(v_buf);
//                }
//                else { // primary key
//                  row_data.push_back(key_value);
//
//                  if ((tb_info_ptr->columns[col_index].values != NULL)
//                      && (tb_it->first != TPCC_TB_DISTRICT)
//                      && record_key) {
//                    tb_info_ptr->columns[col_index].values->push_back(key_value);
//                    record_key = false;
//                  }
//                }
//              }
//              else if (tb_info_ptr->columns[col_index].foreign != NULL) {
//                bool use_key_value = false;
//                int n;
//                size_t fc_size =
//                    tb_info_ptr->columns[col_index].foreign->name.size();
//                std::string last4 =
//                    tb_info_ptr->columns[col_index].foreign->name.substr(
//                        fc_size - 4,
//                        4);
//
//                if (last4 == "i_id") {
//                  n = tb_infos_[std::string(TPCC_TB_ITEM)].num_records;
//                }
//                else if (last4 == "w_id") {
//                  n = tb_infos_[std::string(TPCC_TB_WAREHOUSE)].num_records *
//                      Config::GetConfig()->get_num_site();
//                }
//                else if (last4 == "c_id") {
//                  if (tb_info_ptr->columns[col_index].name ==
//                      "o_c_id")
//                    use_key_value = true;
//                  else
//                    n = tb_infos_[std::string(TPCC_TB_CUSTOMER)].num_records /
//                        tb_infos_[std::string(TPCC_TB_DISTRICT)].num_records;
//                }
//                else if (last4 == "d_id") {
//                  n = tb_infos_[std::string(TPCC_TB_DISTRICT)].num_records /
//                      tb_infos_[std::string(TPCC_TB_WAREHOUSE)].num_records;
//                }
//                else {
//                  n = tb_info_ptr->columns[col_index].foreign_tb->num_records;
//                }
//                Value v_buf;
//
//                if (use_key_value) {
//                  v_buf = key_value;
//                }
//                else {
//                  v_buf = value_get_n(tb_info_ptr->columns[col_index].type,
//                                      RandomGenerator::rand(0, n - 1));
//                }
//                row_data.push_back(v_buf);
//              }
//              else {
//                Value
//                    v_buf = random_value(tb_info_ptr->columns[col_index].type);
//
//                if (tb_info_ptr->columns[col_index].name ==
//                    "d_next_o_id")
//                  v_buf =
//                      Value((i32) (
//                          tb_infos_[std::string(TPCC_TB_ORDER)].num_records /
//                              tb_infos_[std::string(TPCC_TB_DISTRICT)].
//                                  num_records)); // XXX
//
//                if (tb_info_ptr->columns[col_index].name ==
//                    "c_last")
//                  v_buf =
//                      Value(RandomGenerator::int2str_n(
//                          key_value.get_i32() % 1000, 3));
//                row_data.push_back(v_buf);
//              }
//            }
//
//            if (col_index == tb_info_ptr->columns.size()) {
//              if (tb_it->first == TPCC_TB_STOCK) {
//                int key_size;
//                key_size = schema->key_columns_id().size();
//                MultiValue mv(key_size);
//
//                for (int i = 0; i < key_size;
//                     i++)
//                  mv[i] = row_data[schema->key_columns_id()[i]];
//
//                if (sid == site_from_key(mv, tb_info_ptr)) {
//                  counter++;
//
//                  switch (mode) {
//                    case MODE_2PL:
//                      table_ptr->insert(mdb::FineLockedRow::create(schema,
//                                                                   row_data));
//                      break;
//
//                    case MODE_NONE: // FIXME
//                    case MODE_OCC:
//                      table_ptr->insert(mdb::VersionedRow::create(schema,
//                                                                  row_data));
//                      break;
//
//                    case MODE_RCC:
//                      table_ptr->insert(RCCRow::create(schema, row_data));
//                      break;
//
//                    case MODE_RO6:
//                      table_ptr->insert(RO6Row::create(schema, row_data));
//                      break;
//
//                    default:
//                      verify(0);
//                  }
//
//                  // log
//                  // std::string buf;
//                  // for (int i = 0; i < tb_info_ptr->columns.size(); i++)
//                  //
//                  //  buf.append(tb_info_ptr->columns[i].name).append(":").append(to_string(row_data[i])).append(";
//                  // ");
//                  // Log_debug("%s", buf.c_str());
//                }
//              }
//              else if (tb_it->first == TPCC_TB_DISTRICT) {
//                int key_size;
//                key_size = schema->key_columns_id().size();
//                MultiValue mv(key_size);
//
//                for (int i = 0; i < key_size;
//                     i++)
//                  mv[i] = row_data[schema->key_columns_id()[i]];
//
//                if (sid == site_from_key(mv, tb_info_ptr)) {
//                  counter++;
//
//                  switch (mode) {
//                    case MODE_2PL:
//                      table_ptr->insert(mdb::FineLockedRow::create(schema,
//                                                                   row_data));
//                      break;
//
//                    case MODE_NONE: // FIXME
//                    case MODE_OCC:
//                      table_ptr->insert(mdb::VersionedRow::create(schema,
//                                                                  row_data));
//                      break;
//
//                    case MODE_RCC:
//                      table_ptr->insert(RCCRow::create(schema, row_data));
//                      break;
//
//                    case MODE_RO6:
//                      table_ptr->insert(RO6Row::create(schema, row_data));
//                      break;
//
//                    default:
//                      verify(0);
//                  }
//
//                  for (int i = 0; i < key_size;
//                       i++)
//                    tb_info_ptr->columns[schema->key_columns_id()[i]].
//                            values
//                        ->push_back(mv[i]);
//
//                  // log
//                  // std::string buf;
//                  // for (int i = 0; i < tb_info_ptr->columns.size(); i++)
//                  //
//                  //  buf.append(tb_info_ptr->columns[i].name).append(":").append(to_string(row_data[i])).append(";
//                  // ");
//                  // Log_debug("%s", buf.c_str());
//                }
//              }
//              else {
//                counter++;
//                mdb::Row *r = NULL;
//
//                switch (mode) {
//                  case MODE_2PL:
//                    r = mdb::FineLockedRow::create(schema, row_data);
//                    break;
//
//                  case MODE_NONE: // FIXME
//                  case MODE_OCC:
//                    r = mdb::VersionedRow::create(schema, row_data);
//                    break;
//
//                  case MODE_RCC:
//                    r = RCCRow::create(schema, row_data);
//                    break;
//
//                  case MODE_RO6:
//                    r = RO6Row::create(schema, row_data);
//                    break;
//
//                  default:
//                    verify(0);
//                }
//                table_ptr->insert(r);
//
//                // XXX c_last secondary index
//                if (tb_it->first == TPCC_TB_CUSTOMER) {
//                  std::string c_last_buf = r->get_column(
//                      "c_last").get_str();
//                  rrr::i32 c_id_buf = r->get_column(
//                      "c_id").get_i32();
//                  size_t mb_size =
//                      g_c_last_schema.key_columns_id().size(), mb_i = 0;
//                  mdb::MultiBlob mb_buf(mb_size);
//                  mdb::Schema::iterator col_info_it = g_c_last_schema.begin();
//
//                  for (; col_info_it != g_c_last_schema.end(); col_info_it++) {
//                    mb_buf[mb_i++] = r->get_blob(col_info_it->name);
//                  }
//                  g_c_last2id.insert(std::make_pair(c_last_id_t(c_last_buf,
//                                                                mb_buf,
//                                                                &g_c_last_schema),
//                                                    c_id_buf));
//                } // XXX
//
//                // log
//                // std::string buf;
//                // for (int i = 0; i < tb_info_ptr->columns.size(); i++)
//                //
//                //  buf.append(tb_info_ptr->columns[i].name).append(":").append(to_string(row_data[i])).append(";
//                // ");
//                // Log_debug("%s", buf.c_str());
//              }
//
//              if ((num_self_primary == 0) &&
//                  (counter >= tb_info_ptr->num_records)) {
//                num_self_primary = 1;
//                break;
//              }
//            }
//
//            if (num_self_primary == 0) {
//              if (0 !=
//                  index_increase(prim_foreign_index,
//                                 bound_foreign_index))
//                verify(0);
//            }
//            else if (0 !=
//                index_increase(prim_foreign_index,
//                               bound_foreign_index))
//              break;
//          }
//        }
//
//        // Log::debug("end:%s", tb_it->first.c_str());
//        tb_info_ptr->populated[sid] = true;
//        number2populate--;
//
//        // finish populate one table
//      }
//    }
//
//    verify(pre_num2populate > number2populate);
//  }
//
//  release_foreign_values();
//
//  return 0;
//}

//int Sharding::do_tpcc_populate_table(const std::vector<std::string> &table_names,
//                                     uint32_t sid) {
//  int32_t mode = Config::GetConfig()->get_mode();
//  uint32_t number2populate = tb_infos_.size();
//
//  while (number2populate > 0) {
//    uint32_t pre_num2populate = number2populate;
//    std::map<std::string, tb_info_t>::iterator tb_it = tb_infos_.begin();
//
//    for (; tb_it != tb_infos_.end(); tb_it++) {
//      if (!tb_it->second.populated[sid] && Ready2Populate(&(tb_it->second))) {
//        tb_info_t *tb_info_ptr = &(tb_it->second);
//        mdb::Table *const table_ptr = DTxnMgr::GetTxnMgr(sid)->get_table(
//            tb_it->first);
//        const mdb::Schema *schema = table_ptr->schema();
//        verify(schema->columns_count() == tb_info_ptr->columns.size());
//
//        uint32_t col_index = 0;
//
//        if (tb_it->first == TPCC_TB_WAREHOUSE) { // warehouse table
//          Value key_value, max_key;
//          mdb::Schema::iterator col_it = schema->begin();
//
//          for (col_index = 0; col_index < tb_info_ptr->columns.size();
//               col_index++) {
//            verify(col_it != schema->end());
//            verify(tb_info_ptr->columns[col_index].name == col_it->name);
//            verify(tb_info_ptr->columns[col_index].type == col_it->type);
//
//            if (tb_info_ptr->columns[col_index].is_primary) {
//              verify(col_it->indexed);
//              key_value = value_get_zero(tb_info_ptr->columns[col_index].type);
//              max_key = value_get_n(tb_info_ptr->columns[col_index].type,
//                                    tb_info_ptr->num_records *
//                                        tb_info_ptr->num_site);
//            }
//            col_it++;
//          }
//          verify(col_it == schema->end());
//          std::vector<Value> row_data;
//
//          for (; key_value < max_key; ++key_value) {
//            row_data.clear();
//
//            for (col_index = 0; col_index < tb_info_ptr->columns.size();
//                 col_index++) {
//              if (tb_info_ptr->columns[col_index].is_primary) {
//                if (sid != site_from_key(key_value, tb_info_ptr)) break;
//
//                if (tb_info_ptr->columns[col_index].values !=
//                    NULL)
//                  tb_info_ptr->columns[col_index].values->push_back(
//                      key_value);
//                row_data.push_back(key_value);
//
//                // Log_debug("%s (primary): %s",
//                // tb_info_ptr->columns[col_index].name.c_str(),
//                // to_string(key_value).c_str());
//                // std::cerr << tb_info_ptr->columns[col_index].name <<
//                // "(primary):" << row_data.back() << "; ";
//              } else if (tb_info_ptr->columns[col_index].foreign != NULL) {
//                // TODO (ycui) use RandomGenerator
//                Log_fatal("Table %s shouldn't have a foreign key!",
//                          TPCC_TB_WAREHOUSE);
//                verify(0);
//              } else {
//                // TODO (ycui) use RandomGenerator
//                Value
//                    v_buf = random_value(tb_info_ptr->columns[col_index].type);
//                row_data.push_back(v_buf);
//              }
//            }
//
//            if (col_index == tb_info_ptr->columns.size()) {
//              // log
//              // std::string buf;
//              // for (int i = 0; i < tb_info_ptr->columns.size(); i++)
//              //
//              //  buf.append(tb_info_ptr->columns[i].name).append(":").append(to_string(row_data[i])).append(";
//              // ");
//              // rrr::Log::info("%s", buf.c_str());
//              switch (mode) {
//                case MODE_2PL:
//                  table_ptr->insert(mdb::FineLockedRow::create(schema,
//                                                               row_data));
//                  break;
//
//                case MODE_NONE: // FIXME
//                case MODE_OCC:
//                  table_ptr->insert(mdb::VersionedRow::create(schema,
//                                                              row_data));
//                  break;
//
//                case MODE_RCC:
//                  table_ptr->insert(RCCRow::create(schema, row_data));
//                  break;
//
//                case MODE_RO6:
//                  table_ptr->insert(RO6Row::create(schema, row_data));
//                  break;
//
//                default:
//                  verify(0);
//              }
//
//              // Log_debug("Row inserted");
//            }
//          }
//        } else { // non warehouse tables
//          uint64_t num_foreign_row = 1;
//          uint64_t num_self_primary = 0;
//          uint32_t self_primary_col = 0;
//          bool self_primary_col_find = false;
//          std::map<uint32_t,
//                   std::pair<uint32_t, uint32_t> > prim_foreign_index;
//          mdb::Schema::iterator col_it = schema->begin();
//
//          mdb::SortedTable *tbl_sec_ptr = NULL;
//
//          if (tb_it->first ==
//              TPCC_TB_ORDER)
//            tbl_sec_ptr =
//                (mdb::SortedTable *) DTxnMgr::GetTxnMgr(sid)->get_table(
//                    TPCC_TB_ORDER_C_ID_SECONDARY);
//
//          for (col_index = 0; col_index < tb_info_ptr->columns.size();
//               col_index++) {
//            verify(col_it != schema->end());
//            verify(tb_info_ptr->columns[col_index].name == col_it->name);
//            verify(tb_info_ptr->columns[col_index].type == col_it->type);
//
//            if (tb_info_ptr->columns[col_index].is_primary) {
//              verify(col_it->indexed);
//
//              if (tb_it->first == TPCC_TB_CUSTOMER) { // XXX
//                if (col_it->name != "c_id")
//                  g_c_last_schema.add_column(
//                      col_it->name.c_str(),
//                      col_it->type,
//                      true);
//              } // XXX
//
//              if (tb_info_ptr->columns[col_index].foreign_tb != NULL) {
//                uint32_t tmp_int;
//
//                if (tb_info_ptr->columns[col_index].foreign->name == "i_id") {
//
//                  // refers to item.i_id, use all available i_id
//                  // instead of local i_id
//                  tmp_int =
//                      tb_info_ptr->columns[col_index].foreign_tb->num_records;
//
//                  if (tb_info_ptr->columns[col_index].values != NULL) {
//                    tb_info_ptr->columns[col_index].values->resize(tmp_int);
//
//                    for (i32 i = 0; i < tmp_int;
//                         i++)
//                      (*tb_info_ptr->columns[col_index].values)[i] =
//                          Value(i);
//                  }
//                }
//                else {
//                  column_t *foreign_column =
//                      tb_info_ptr->columns[col_index].foreign;
//                  tmp_int = foreign_column->values->size();
//
//                  if (tb_info_ptr->columns[col_index].values != NULL) {
//                    tb_info_ptr->columns[col_index].values->assign(
//                        foreign_column->values->begin(),
//                        foreign_column->values->end());
//                  }
//                }
//                verify(tmp_int > 0);
//                prim_foreign_index[col_index] =
//                    std::pair<uint32_t, uint32_t>(0, tmp_int);
//                num_foreign_row *= tmp_int;
//              }
//              else {
//                // only one primary key can refer to no other table.
//                verify(!self_primary_col_find);
//                self_primary_col = col_index;
//                self_primary_col_find = true;
//              }
//            }
//            col_it++;
//          }
//
//          if (tb_it->first == TPCC_TB_CUSTOMER) { // XXX
//            g_c_last_schema.add_column("c_id", mdb::Value::I32, true);
//          }                                       // XXX
//          // TODO (ycui) add a vector in
//          // tb_info_t to record used
//          // values for key.
//          verify(tb_info_ptr->num_records % num_foreign_row == 0
//                     || tb_info_ptr->num_records < num_foreign_row);
//
//          // Log_debug("foreign row: %llu, this row: %llu", num_foreign_row,
//          // tb_info_ptr->num_records);
//          num_self_primary = tb_info_ptr->num_records / num_foreign_row;
//          Value key_value = value_get_zero(
//              tb_info_ptr->columns[self_primary_col].type);
//          Value max_key = value_get_n(
//              tb_info_ptr->columns[self_primary_col].type,
//              num_self_primary);
//          std::vector<Value> row_data;
//
//          // Log_debug("Begin primary key: %s, Max primary key: %s",
//          // to_string(key_value).c_str(), to_string(max_key).c_str());
//          for (; key_value < max_key || num_self_primary == 0; ++key_value) {
//            bool record_key = true;
//            init_index(prim_foreign_index);
//            int counter = 0;
//
//            while (true) {
//              row_data.clear();
//
//              for (col_index = 0; col_index < tb_info_ptr->columns.size();
//                   col_index++) {
//                if (tb_info_ptr->columns[col_index].is_primary) {
//                  if (prim_foreign_index.size() == 0) {
//                    if (sid != site_from_key(key_value, tb_info_ptr)) break;
//                    row_data.push_back(key_value);
//
//                    if (tb_info_ptr->columns[col_index].values != NULL) {
//                      tb_info_ptr->columns[col_index].values->push_back(
//                          key_value);
//                    }
//
//                    // Log_debug("%s (primary): %s",
//                    // tb_info_ptr->columns[col_index].name.c_str(),
//                    // to_string(key_value).c_str());
//                  }
//
//                    // primary key and foreign key
//                  else if (tb_info_ptr->columns[col_index].foreign != NULL) {
//                    Value v_buf;
//
//                    if (tb_info_ptr->columns[col_index].foreign->name ==
//                        "i_id")
//                      v_buf = Value(
//                          (i32) prim_foreign_index[col_index].first);
//                    else
//                      v_buf =
//                          (*tb_info_ptr->columns[col_index].foreign->values)[
//                              prim_foreign_index[col_index].first];
//
//                    // Log_debug("%s (primary, foreign): %s",
//                    // tb_info_ptr->columns[col_index].name.c_str(),
//                    // to_string(v_buf).c_str());
//                    row_data.push_back(v_buf);
//                  }
//                  else { // primary key
//                    row_data.push_back(key_value);
//
//                    if ((tb_info_ptr->columns[col_index].values != NULL)
//                        && record_key) {
//                      tb_info_ptr->columns[col_index].values->push_back(
//                          key_value);
//                      record_key = false;
//                    }
//
//                    // Log_debug("%s (primary): %s",
//                    // tb_info_ptr->columns[col_index].name.c_str(),
//                    // to_string(key_value).c_str());
//                  }
//                }
//                else if (tb_info_ptr->columns[col_index].foreign != NULL) {
//                  bool use_key_value = false;
//                  int n;
//                  size_t fc_size =
//                      tb_info_ptr->columns[col_index].foreign->name.size();
//                  std::string last4 =
//                      tb_info_ptr->columns[col_index].foreign->name.substr(
//                          fc_size - 4,
//                          4);
//
//                  if (last4 == "i_id") {
//                    n = tb_infos_[std::string(TPCC_TB_ITEM)].num_records;
//                  }
//                  else if (last4 == "w_id") {
//                    n = tb_infos_[std::string(TPCC_TB_WAREHOUSE)].num_records *
//                        Config::GetConfig()->get_num_site();
//                  }
//                  else if (last4 == "c_id") {
//                    if (tb_info_ptr->columns[col_index].name ==
//                        "o_c_id")
//                      use_key_value = true;
//                    else
//                      n = tb_infos_[std::string(TPCC_TB_CUSTOMER)].num_records /
//                          tb_infos_[std::string(TPCC_TB_DISTRICT)].num_records;
//                  }
//                  else if (last4 == "d_id") {
//                    n = tb_infos_[std::string(TPCC_TB_DISTRICT)].num_records /
//                        tb_infos_[std::string(TPCC_TB_WAREHOUSE)].num_records;
//                  }
//                  else {
//                    n = tb_info_ptr->columns[col_index].foreign_tb->num_records;
//                  }
//                  Value v_buf;
//
//                  if (use_key_value) v_buf = key_value;
//                  else
//                    v_buf = value_get_n(tb_info_ptr->columns[col_index].type,
//                                        RandomGenerator::rand(0, n - 1));
//                  row_data.push_back(v_buf);
//
//                  // Log_debug("%s (foreign): %s",
//                  // tb_info_ptr->columns[col_index].name.c_str(),
//                  // to_string(v_buf).c_str());
//                }
//                else {
//                  Value v_buf =
//                      random_value(tb_info_ptr->columns[col_index].type);
//
//                  if (tb_info_ptr->columns[col_index].name ==
//                      "d_next_o_id")
//                    v_buf =
//                        Value((i32) (
//                            tb_infos_[std::string(TPCC_TB_ORDER)].num_records
//                                / tb_infos_[std::string(TPCC_TB_DISTRICT)].
//                                    num_records)); // XXX
//
//                  if (tb_info_ptr->columns[col_index].name ==
//                      "c_last")
//                    v_buf =
//                        Value(RandomGenerator::int2str_n(
//                            key_value.get_i32() % 1000,
//                            3));
//                  row_data.push_back(v_buf);
//
//                  // Log_debug("%s: %s",
//                  // tb_info_ptr->columns[col_index].name.c_str(),
//                  // to_string(v_buf).c_str());
//                }
//              }
//
//              if (col_index == tb_info_ptr->columns.size()) {
//                // log
//                // std::string buf;
//                // for (int i = 0; i < tb_info_ptr->columns.size(); i++)
//                //
//                //  buf.append(tb_info_ptr->columns[i].name).append(":").append(to_string(row_data[i])).append(";
//                // ");
//                // rrr::Log::info("%s", buf.c_str());
//                mdb::Row *r = NULL;
//
//                switch (mode) {
//                  case MODE_2PL:
//                    r = mdb::FineLockedRow::create(schema, row_data);
//                    break;
//
//                  case MODE_NONE: // FIXME
//                  case MODE_OCC:
//                    r = mdb::VersionedRow::create(schema, row_data);
//                    break;
//
//                  case MODE_RCC:
//                    r = RCCRow::create(schema, row_data);
//                    break;
//
//                  case MODE_RO6:
//                    r = RO6Row::create(schema, row_data);
//                    break;
//
//                  default:
//                    verify(0);
//                }
//                table_ptr->insert(r);
//
//                //
//                if (tbl_sec_ptr) {
//                  rrr::i32 cur_o_id_buf = r->get_column("o_id").get_i32();
//                  const mdb::Schema *sch_buf = tbl_sec_ptr->schema();
//                  mdb::MultiBlob mb_buf(sch_buf->key_columns_id().size());
//                  mdb::Schema::iterator col_info_it = sch_buf->begin();
//                  size_t mb_i = 0;
//
//                  for (; col_info_it != sch_buf->end(); col_info_it++)
//                    if (col_info_it->indexed)
//                      mb_buf[mb_i++] = r->get_blob(
//                          col_info_it->name);
//                  mdb::SortedTable::Cursor rs = tbl_sec_ptr->query(mb_buf);
//
//                  if (rs.has_next()) {
//                    mdb::Row *r_buf = rs.next();
//                    rrr::i32 o_id_buf = r_buf->get_column("o_id").get_i32();
//
//                    if (o_id_buf < cur_o_id_buf)
//                      r_buf->update("o_id",
//                                    cur_o_id_buf);
//                  }
//                  else {
//                    std::vector<Value> sec_row_data_buf;
//
//                    for (col_info_it = sch_buf->begin();
//                         col_info_it != sch_buf->end();
//                         col_info_it++)
//                      sec_row_data_buf.push_back(r->get_column(
//                          col_info_it->
//                              name));
//                    mdb::Row *r_buf = NULL;
//
//                    switch (mode) {
//                      case MODE_2PL:
//                        r_buf = mdb::FineLockedRow::create(sch_buf,
//                                                           sec_row_data_buf);
//                        break;
//
//                      case MODE_NONE: // FIXME
//                      case MODE_OCC:
//                        r_buf = mdb::VersionedRow::create(sch_buf,
//                                                          sec_row_data_buf);
//                        break;
//
//                      case MODE_RCC:
//                        r_buf = RCCRow::create(sch_buf, sec_row_data_buf);
//                        break;
//
//                      case MODE_RO6:
//                        r_buf = RO6Row::create(sch_buf, sec_row_data_buf);
//                        break;
//
//                      default:
//                        verify(0);
//                    }
//                    tbl_sec_ptr->insert(r_buf);
//                  }
//                }
//
//
//                // XXX c_last secondary index
//                if (tb_it->first == TPCC_TB_CUSTOMER) {
//                  std::string c_last_buf = r->get_column(
//                      "c_last").get_str();
//                  rrr::i32 c_id_buf = r->get_column(
//                      "c_id").get_i32();
//                  size_t mb_size =
//                      g_c_last_schema.key_columns_id().size(), mb_i = 0;
//                  mdb::MultiBlob mb_buf(mb_size);
//                  mdb::Schema::iterator col_info_it = g_c_last_schema.begin();
//
//                  for (; col_info_it != g_c_last_schema.end(); col_info_it++) {
//                    mb_buf[mb_i++] = r->get_blob(col_info_it->name);
//                  }
//                  g_c_last2id.insert(std::make_pair(c_last_id_t(c_last_buf,
//                                                                mb_buf,
//                                                                &g_c_last_schema),
//                                                    c_id_buf));
//                } // XXX
//
//                // Log_debug("Row inserted");
//                counter++;
//
//                if ((num_self_primary == 0) &&
//                    (counter >= tb_info_ptr->num_records)) {
//                  num_self_primary = 1;
//                  break;
//                }
//              }
//
//              if (num_self_primary == 0) {
//                if (0 != index_increase(prim_foreign_index)) verify(0);
//              }
//              else if (0 != index_increase(prim_foreign_index)) break;
//            }
//          }
//        }
//
//        tb_info_ptr->populated[sid] = true;
//        number2populate--;
//
//        // finish populate one table
//      }
//    }
//
//    verify(pre_num2populate > number2populate);
//  }
//
//  release_foreign_values();
//
//  return 0;
//}

//int Sharding::do_populate_table(const std::vector<std::string> &table_names,
//                                uint32_t sid) {
//  int mode = Config::GetConfig()->get_mode();
//  uint32_t number2populate = tb_infos_.size();
//
//  while (number2populate > 0) {
//    std::map<std::string, tb_info_t>::iterator tb_it = tb_infos_.begin();
//
//    for (; tb_it != tb_infos_.end(); tb_it++) {
//      if (!tb_it->second.populated[sid]) {
//        tb_info_t *tb_info_ptr = &(tb_it->second);
//
//        mdb::Table *const table_ptr = DTxnMgr::GetTxnMgr(sid)->get_table(
//            tb_it->first);
//
//        if (table_ptr == NULL) {
//          tb_info_ptr->populated[sid] = true;
//          number2populate--;
//          continue;
//        }
//        const mdb::Schema *schema = table_ptr->schema();
//        verify(schema->columns_count() == tb_info_ptr->columns.size());
//
//        Value key_value, max_key;
//        mdb::Schema::iterator col_it = schema->begin();
//        uint32_t col_index = 0;
//
//        for (; col_index < tb_info_ptr->columns.size(); col_index++) {
//          verify(col_it != schema->end());
//          verify(tb_info_ptr->columns[col_index].name == col_it->name);
//          verify(tb_info_ptr->columns[col_index].type == col_it->type);
//
//          if (tb_info_ptr->columns[col_index].is_primary) {
//            verify(col_it->indexed);
//            key_value = value_get_zero(tb_info_ptr->columns[col_index].type);
//            max_key = value_get_n(tb_info_ptr->columns[col_index].type,
//                                  tb_info_ptr->num_records);
//          }
//          col_it++;
//        }
//
//        std::vector<Value> row_data;
//
//        for (; key_value < max_key; ++key_value) {
//          row_data.clear();
//
//          for (col_index = 0; col_index < tb_info_ptr->columns.size();
//               col_index++) {
//            if (tb_info_ptr->columns[col_index].is_primary) {
//              // if (tb_info_ptr->columns[col_index].values != NULL)
//              //
//              //  tb_info_ptr->columns[col_index].values->push_back(key_value);
//              if (sid != site_from_key(key_value, tb_info_ptr)) break;
//              row_data.push_back(key_value);
//
//              // std::cerr << tb_info_ptr->columns[col_index].name <<
//              // "(primary):" << row_data.back() << "; ";
//            } else if (tb_info_ptr->columns[col_index].foreign != NULL) {
//              Value v_buf = value_get_n(tb_info_ptr->columns[col_index].type,
//                                        RandomGenerator::rand(0,
//                                                              tb_info_ptr->columns
//                                                              [col_index].
//                                                                  foreign_tb->
//                                                                  num_records
//                                                                  - 1));
//              row_data.push_back(v_buf);
//
//              // std::cerr << tb_info_ptr->columns[col_index].name <<
//              // "(foreign:" << tb_info_ptr->columns[col_index].foreign->name <<
//              // "):" << v_buf << "; ";
//            } else {
//              Value v_buf;
//
//              // TODO (ycui) use RandomGenerator
//              v_buf = random_value(tb_info_ptr->columns[col_index].type);
//              row_data.push_back(v_buf);
//
//              // std::cerr << tb_info_ptr->columns[col_index].name << ":" <<
//              // v_buf << "; ";
//            }
//          }
//
//          if (col_index == tb_info_ptr->columns.size()) {
//            switch (mode) {
//              case MODE_2PL:
//                table_ptr->insert(mdb::FineLockedRow::create(schema, row_data));
//                break;
//
//              case MODE_NONE:
//              case MODE_RPC_NULL:
//              case MODE_OCC:
//                table_ptr->insert(mdb::VersionedRow::create(schema, row_data));
//                break;
//
//              case MODE_RCC:
//                table_ptr->insert(RCCRow::create(schema, row_data));
//                break;
//
//              case MODE_RO6:
//                table_ptr->insert(RO6Row::create(schema, row_data));
//                break;
//
//              default:
//                verify(0);
//            }
//          }
//        }
//
//        tb_info_ptr->populated[sid] = true;
//        number2populate--;
//      }
//    }
//  }
//  release_foreign_values();
//  return 0;
//}

int Sharding::get_number_rows(std::map<std::string, uint64_t> &table_map) {

  for (auto it = tb_infos_.begin(); it != tb_infos_.end(); it++)
    table_map[it->first] = (uint64_t) (it->second.num_records);
  return 0;
}

void Sharding::release_foreign_values() {
  std::map<std::string, tb_info_t>::iterator tb_it = tb_infos_.begin();
  std::vector<column_t>::iterator c_it;

  for (; tb_it != tb_infos_.end(); tb_it++)
    for (c_it =
             tb_it->second.columns.begin(); c_it != tb_it->second.columns.end();
         c_it++)
      if (c_it->values != NULL) {
        delete c_it->values;
        c_it->values = NULL;
      }
}

int init_index(std::map<uint32_t, std::pair<uint32_t,
                                            uint32_t> > &index) {
  if (index.size() == 0) return -1;

  std::map<uint32_t,
           std::pair<uint32_t, uint32_t> >::iterator it = index.begin();

  for (; it != index.end(); it++) {
    verify(it->second.second > 0);
    it->second.first = 0;
  }
  return 0;
}

int index_reverse_increase(std::map<uint32_t, std::pair<uint32_t,
                                                        uint32_t> > &index,
                           const std::vector<uint32_t> &bound_index) {
  if (bound_index.size() <= 1) return index_reverse_increase(index);

  std::vector<uint32_t>::const_iterator it = bound_index.begin();
  it++;

  for (; it != bound_index.end(); it++) index.erase(*it);
  int ret = index_reverse_increase(index);
  it = bound_index.begin();
  it++;

  for (; it != bound_index.end(); it++) index[*it] = index[bound_index[0]];
  return ret;
}

int index_increase(std::map<uint32_t, std::pair<uint32_t,
                                                uint32_t> > &index,
                   const std::vector<uint32_t> &bound_index) {
  if (bound_index.size() <= 1) return index_increase(index);

  std::vector<uint32_t>::const_iterator it = bound_index.begin();
  it++;

  for (; it != bound_index.end(); it++) index.erase(*it);
  int ret = index_increase(index);
  it = bound_index.begin();
  it++;

  for (; it != bound_index.end(); it++) index[*it] = index[bound_index[0]];
  return ret;
}

int index_reverse_increase(std::map<uint32_t, std::pair<uint32_t,
                                                        uint32_t> > &index) {
  if (index.size() == 0) return -1;

  std::map<uint32_t,
           std::pair<uint32_t,
                     uint32_t> >::reverse_iterator it = index.rbegin();
  it->second.first++;

  while (it->second.first >= it->second.second) {
    it->second.first -= it->second.second;
    verify(it->second.first == 0);

    it++;

    if (it == index.rend()) return -1;

    it->second.first++;
  }

  return 0;
}

int index_increase(std::map<uint32_t, std::pair<uint32_t,
                                                uint32_t> > &index) {
  if (index.size() == 0) return -1;

  std::map<uint32_t,
           std::pair<uint32_t, uint32_t> >::iterator it = index.begin();
  it->second.first++;

  while (it->second.first >= it->second.second) {
    it->second.first -= it->second.second;
    verify(it->second.first == 0);

    it++;

    if (it == index.end()) return -1;

    it->second.first++;
  }

  return 0;
}

Value random_value(Value::kind k) {
  switch (k) {
    case Value::I32:
      return Value((i32) RandomGenerator::rand());

    case Value::I64:
      return Value((i64) RandomGenerator::rand());

    case Value::DOUBLE:
      return Value(RandomGenerator::rand_double());

    case Value::STR:
      return Value(RandomGenerator::int2str_n(RandomGenerator::rand(0, 999),
                                              3));

    case Value::UNKNOWN:
    default:
      verify(0);
      return Value();
  }
}

Value value_get_zero(Value::kind k) {
  switch (k) {
    case Value::I32:
      return Value((i32) 0);

    case Value::I64:
      return Value((i64) 0);

    case Value::DOUBLE:
      return Value((double) 0.0);

    case Value::STR:

      // TODO (ycui) str zero
      verify(0);
      return Value(std::string(""));

    case Value::UNKNOWN:
      verify(0);
      return Value(std::string(""));

    default:
      verify(0);
      return Value(std::string(""));
  }
}

Value value_get_n(Value::kind k, int v) {
  switch (k) {
    case Value::I32:
      return Value((i32) v);

    case Value::I64:
      return Value((i64) v);

    case Value::DOUBLE:
      return Value((double) v);

    case Value::STR:
      return Value(std::to_string(v));

    case Value::UNKNOWN:
      verify(0);
      return Value(std::string(""));

    default:
      verify(0);
      return Value(std::string(""));
  }
}

Value &operator++(Value &rhs) {
  rhs++;
  return rhs;
}

Value operator++(Value &lhs, int) {
  Value ret = lhs;

  switch (lhs.get_kind()) {
    case Value::I32:
      lhs.set_i32(lhs.get_i32() + 1);
      break;

    case Value::I64:
      lhs.set_i64(lhs.get_i64() + 1);
      break;

    case Value::DOUBLE:
      lhs.set_double(lhs.get_double() + 1.0);
      break;

    case Value::STR:

      // TODO (ycui) str increment
      verify(0);
      break;

    case Value::UNKNOWN:
      verify(0);
      break;

    default:
      verify(0);
      break;
  }
  return ret;
}

Value value_rr_get_next(const std::string &s,
                        Value::kind k,
                        int max,
                        int start) {
  static std::map<std::string, Value> value_map;
  std::map<std::string, Value>::iterator it;

  it = value_map.find(s);

  if (it == value_map.end()) {
    Value buf = value_get_n(k, start);
    value_map[s] = buf;
    return buf;
  }
  else {
    it->second++;

    if (it->second > value_get_n(k, max)) it->second = value_get_n(k, start);
    return it->second;
  }
}
}
