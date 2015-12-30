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
                           const std::vector<uint32_t>& site_id) {
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
                               const std::vector<uint32_t>& site_id) {
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
  if (it->second.site_id.size() == 0) return -2;
  site_id = site_from_key(key, &(it->second));
  return 0;
}

int Sharding::get_site_id_from_tb(const std::string &tb_name,
                                  std::vector<uint32_t> &site_id) {
  std::map<std::string, tb_info_t>::iterator it = tb_infos_.find(tb_name);
  if (it == tb_infos_.end()) return -1;
  if (it->second.site_id.size() == 0) return -2;

  site_id.clear();
  site_id.insert(site_id.end(), it->second.site_id.begin(), it->second.site_id.end());
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
int Sharding::PopulateTable(uint32_t partition_id) {
  fprintf(stderr, "%s not implemented\n", __FUNCTION__);
  verify(0);
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
