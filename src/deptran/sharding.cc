#include "constants.h"
#include "sharding.h"
#include "scheduler.h"

// for tpca benchmark
#include "bench/tpca/workload.h"
#include "bench/tpca/payment.h"

// tpcc benchmark
#include "bench/tpcc/workload.h"
#include "bench/tpcc/procedure.h"

// tpcc dist partition benchmark
#include "bench/tpcc_dist/procedure.h"

// tpcc real dist partition benchmark
#include "bench/tpcc_real_dist/workload.h"
#include "bench/tpcc_real_dist/procedure.h"

// rw benchmark
#include "bench/rw/workload.h"
#include "bench/rw/procedure.h"

// micro bench
#include "bench/micro/workload.h"
#include "bench/micro/procedure.h"

namespace janus {

Sharding::Sharding() { }

Sharding::Sharding(const Sharding &sharding)
    : tb_infos_(sharding.tb_infos_), dist2sid_(sharding.dist2sid_),
      stock2sid_(sharding.stock2sid_) {
  BuildTableInfoPtr();
}

Sharding::~Sharding() {
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

parid_t Sharding::PartitionFromKey(const MultiValue &key,
                                   const tb_info_t *tb_info) {
  const MultiValue &key_buf =
      Config::GetConfig()->benchmark() != TPCC_REAL_DIST_PART ?
      key :
      (tb_info->tb_name == TPCC_TB_STOCK
           || tb_info->tb_name == TPCC_TB_ITEM ?
       stock_mapping(key) :
       (tb_info->tb_name != TPCC_TB_HISTORY ?
        dist_mapping(key) : key));

  const int num_partitions = Config::GetConfig()->replica_groups_.size();
  uint32_t ret;

  switch (tb_info->sharding_method) {
    case MODULUS:
      ret = modulus(key_buf, num_partitions);
      break;

    case INT_MODULUS:
      ret = int_modulus(key_buf, num_partitions);
      break;

    default:
      ret = modulus(key_buf, num_partitions);
      break;
  }
  return ret;
}

uint32_t Sharding::modulus(const MultiValue &key, uint32_t num_partitions) {
  uint32_t index = 0;
  long long int buf;
  int i = 0;

  while (i < key.size()) {
    switch (key[i].get_kind()) {
      case Value::I32:
        buf = key[i].get_i32() % num_partitions;
        index += buf > 0 ? (uint32_t) buf : (uint32_t) (-buf);
        break;

      case Value::I64:
        buf = key[i].get_i64() % num_partitions;
        index += buf > 0 ? (uint32_t) buf : (uint32_t) (-buf);
        break;

      case Value::DOUBLE:
        buf = ((long long int) floor(key[i].get_double())) % num_partitions;
        index += buf > 0 ? (uint32_t) buf : (uint32_t) (-buf);
        break;

      case Value::STR: {
        uint32_t sum = 0;
        const std::string &str_buf = key[i].get_str();
        size_t i = 0;

        for (; i < str_buf.size(); i++) sum += (uint32_t) str_buf[i];
        index += sum % num_partitions;
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
    index %= num_partitions;
  }
  return index % num_partitions;
}

uint32_t Sharding::int_modulus(const MultiValue &key, uint32_t num_partitions) {
  uint32_t index = 0;
  long long int buf;
  int i = 0;

  while (i < key.size()) {
    switch (key[i].get_kind()) {
      case Value::I32:
        buf = key[i].get_i32() % num_partitions;
        index += buf > 0 ? (uint32_t) buf : (uint32_t) (-buf);
        break;

      case Value::I64:
        buf = key[i].get_i64() % num_partitions;
        index += buf > 0 ? (uint32_t) buf : (uint32_t) (-buf);
        break;

      case Value::DOUBLE:
        buf = ((long long int) floor(key[i].get_double())) % num_partitions;
        index += buf > 0 ? (uint32_t) buf : (uint32_t) (-buf);
        break;

      case Value::STR: {
        uint32_t sum = 0;
        uint32_t mod = 1;
        const std::string &str_buf = key[i].get_str();
        std::string::const_reverse_iterator rit = str_buf.rbegin();

        for (; rit != str_buf.rend(); rit++) {
          sum += mod * (uint32_t) (*rit);
          sum %= num_partitions;
          mod *= 127;
          mod %= num_partitions;
        }
        index += sum % num_partitions;
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
    index %= num_partitions;
  }
  return index % num_partitions;
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

int Sharding::GetPartition(const std::string &tb_name,
                           const MultiValue &key,
                           parid_t& par_id) {
  std::map<std::string, tb_info_t>::iterator it = tb_infos_.find(tb_name);
  if (it == tb_infos_.end()) return -1;
  if (it->second.par_ids.size() == 0) return -2;
  par_id = PartitionFromKey(key, &(it->second));
  return 0;
}

std::vector<siteid_t> Sharding::SiteIdsForKey(const std::string &tb_name,
                                              const MultiValue &key) {
  std::map<std::string, tb_info_t>::iterator tb_info_it = tb_infos_.find(tb_name);
  verify(tb_info_it != tb_infos_.end());

  parid_t partition_id = PartitionFromKey(key, &(tb_info_it->second));
  auto sites = Config::GetConfig()->SitesByPartitionId(partition_id);
  std::vector<siteid_t> result;
  std::for_each(sites.begin(), sites.end(), [&result](const Config::SiteInfo& site) {
    result.push_back(site.id);
  });
  return result;
}

int Sharding::GetTablePartitions(const std::string &tb_name,
                                 vector<uint32_t> &par_ids) {
  std::map<std::string, tb_info_t>::iterator it = tb_infos_.find(tb_name);
  if (it == tb_infos_.end()) return -1;
  if (it->second.par_ids.size() == 0) return -2;

  tb_info_t& tbl_info = it->second;
  par_ids.clear();
  par_ids.insert(par_ids.end(),
                 tbl_info.par_ids.begin(),
                 tbl_info.par_ids.end());
  return 0;
}

int Sharding::init_schema(const std::string &tb_name,
                          mdb::Schema *schema,
                          mdb::symbol_t *symbol) {
  auto &tb_infos = tb_infos_;
  std::map<std::string, tb_info_t>::iterator it;

  it = tb_infos.find(tb_name);

  if (it == tb_infos.end()) return -1;
  auto &tb_info = it->second;
  auto column_it = tb_info.columns.begin();

  for (; column_it != tb_info.columns.end(); column_it++) {
    schema->add_column(
        column_it->name.c_str(), column_it->type, column_it->is_primary);
  }
  *symbol = tb_info.symbol;
  return schema->columns_count();
}

int Sharding::GetTableNames(parid_t par_id,
                            vector<string> &tables) {
//  tables.clear();
  verify(tables.size() == 0);
  for (auto it = tb_infos_.begin(); it != tb_infos_.end(); it++) {
    auto &tbl_name = it->first;
    auto &tbl_info = it->second;
    for (auto pid : tbl_info.par_ids) {
      if (pid == par_id) {
        tables.push_back(tbl_name);
        break;
      }
    }
  }
  verify(tables.size() > 0);
  return tables.size();
}

bool Sharding::Ready2Populate(tb_info_t *tb_info) {
  auto &columns = tb_info->columns;
  for (auto c_it = columns.begin(); c_it != columns.end(); c_it++) {
    auto fcol = c_it->foreign;
    if ((fcol != nullptr) &&
        (fcol->values != nullptr) &&
        (fcol->values->size() == 0))
      // have foreign table
      // foreign table has some mysterious values
      // those values have not been put in
      return false;
  }
  return true;
}

int Sharding::PopulateTable(tb_info_t *tb_info,
                            parid_t par_id) {
  // find table and secondary table
  mdb::Table *const table_ptr = tx_sched_->get_table(tb_info->tb_name);
  const mdb::Schema *schema = table_ptr->schema();
  mdb::SortedTable *tbl_sec_ptr = nullptr;

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
  verify(col_it == schema->end());

  // TODO (ycui) add a vector in tb_info_t to record used values for key.
  uint64_t loc_num_records = tb_info->num_records;
  verify(loc_num_records % num_foreign_row == 0 ||
      tb_info->num_records < num_foreign_row);
  num_self_primary = loc_num_records / num_foreign_row;
//  verify(num_self_primary > 0);
  Value key_value = value_get_zero(tb_info->columns[self_primary_col].type);
  Value max_key = value_get_n(tb_info->columns[self_primary_col].type,
                              num_self_primary);
  for (; key_value < max_key || num_self_primary == 0; ++key_value) {
    init_index(prim_foreign_index);
    InsertRow(tb_info, par_id, key_value, schema, table_ptr, tbl_sec_ptr);
  }
  return 0;
}

// TODO this should be moved to per benchmark class
int Sharding::PopulateTables(parid_t par_id) {
  auto n_left = tb_infos_.size();
  verify(n_left > 0);

  do {
    bool populated = false;
    for (auto tb_it = tb_infos_.begin(); tb_it != tb_infos_.end(); tb_it++) {
      tb_info_t *tb_info = &(tb_it->second);
      verify(tb_it->first == tb_info->tb_name);

      // TODO is this unnecessary?
      auto it = tb_info->populated.find(par_id);
      if (it == tb_info->populated.end()) {
        tb_info->populated[par_id] = false;
      }

      if (!tb_info->populated[par_id] &&
          Ready2Populate(tb_info)) {
        PopulateTable(tb_info, par_id);
        tb_info->populated[par_id] = true;
        // finish populate one table
        n_left--;
        populated = true;
      }
    }
    verify(populated);
  } while (n_left > 0);

  release_foreign_values();
  return 0;

  fprintf(stderr, "%s -- implement in subclass\n", __FUNCTION__);
  verify(0);
}


void Sharding::InsertRow(tb_info_t *tb_info,
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

int index_increase(std::map<uint32_t, std::pair<uint32_t, uint32_t> > &index,
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

int index_increase(std::map<uint32_t, std::pair<uint32_t, uint32_t> > &index) {
  if (index.size() == 0) return -1;

  std::map<uint32_t, std::pair<uint32_t, uint32_t> >::iterator it = index.begin();
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
      return Value(RandomGenerator::int2str_n(RandomGenerator::rand(0, 999), 3));

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
