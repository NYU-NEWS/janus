#pragma once
#include "__dep__.h"
#include "multi_value.h"

namespace janus {
class Config;

// TODO These below seem to be in the wrong place.
Value value_get_zero(Value::kind k);

Value value_get_n(Value::kind k,
                  int n);

Value &operator++(Value &lhs);

Value  operator++(Value &lhs,
                  int);

Value random_value(Value::kind k);

Value value_rr_get_next(const std::string &s,
                        Value::kind k,
                        int max,
                        int start = 0);

int init_index(map<uint32_t, std::pair<uint32_t,
                                            uint32_t> > &index);

int index_reverse_increase(map<uint32_t, std::pair<uint32_t,
                                                   uint32_t> > &index);

int index_reverse_increase(map<uint32_t, std::pair<uint32_t,
                                                   uint32_t> > &index,
                           const std::vector<uint32_t> &bound_index);

int index_increase(map<uint32_t, std::pair<uint32_t,
                                                uint32_t> > &index);

int index_increase(map<uint32_t, std::pair<uint32_t,
                                           uint32_t> > &index,
                   const std::vector<uint32_t> &bound_index);


// XXX hardcode for c_last secondary index
typedef struct c_last_id_t {
  std::string c_last;
  mdb::SortedMultiKey c_index_smk;

  c_last_id_t(const std::string &_c_last,
              const mdb::MultiBlob &mb,
              const mdb::Schema *schema)
      : c_last(_c_last), c_index_smk(mb, schema) { }

  bool operator<(const c_last_id_t &rhs) const {
    int ret = strcmp(c_last.c_str(), rhs.c_last.c_str());

    if (ret < 0) return true;
    else if (ret == 0) {
      if (c_index_smk < rhs.c_index_smk) return true;
    }
    return false;
  }
} c_last_id_t;

class TxLogServer;
class Frame;
class Sharding {
 public:

  enum method_t {
    MODULUS,
    INT_MODULUS,
  };

  struct tb_info_t;

  typedef struct column_t {
    column_t(Value::kind _type,
             std::string _name,
             bool _is_primary,
             bool _is_foreign,
             string ftbl_name,
             string fcol_name
    ) : type(_type),
        name(_name),
        is_primary(_is_primary),
        values(nullptr),
        is_foreign(_is_foreign),
        foreign_tbl_name(ftbl_name),
        foreign_col_name(fcol_name),
        foreign_tb(nullptr),
        foreign(nullptr) {
    }

    column_t(const column_t& c)
        : type(c.type),
          name(c.name),
          is_primary(c.is_primary),
          is_foreign(c.is_foreign),
          foreign_tbl_name(c.foreign_tbl_name),
          foreign_col_name(c.foreign_col_name),
          foreign(nullptr),
          foreign_tb(nullptr),
          values(nullptr) {}

    Value::kind type;
    std::string name;
    bool is_primary;
    bool is_foreign;
    string foreign_tbl_name;
    string foreign_col_name;
    column_t *foreign;
    tb_info_t *foreign_tb;
    std::vector<Value> *values; // ? what is this about?
  } column_t;

  struct tb_info_t {
    tb_info_t()
        : sharding_method(MODULUS),
          num_records(0) { }

    tb_info_t(std::string method,
              uint32_t ns = 0,
              std::vector<uint32_t>* pars = nullptr,
              uint64_t _num_records = 0,
              mdb::symbol_t _symbol = mdb::TBL_SORTED
    ) : num_records(_num_records),
        symbol(_symbol) {
      if (pars) {
        par_ids = vector<uint32_t>(*pars);
        verify(0);
      }
      if (method == "modulus") sharding_method = MODULUS;
      else if (method == "int_modulus") sharding_method = INT_MODULUS;
      else sharding_method = MODULUS;
    }

    method_t sharding_method;
    vector<uint32_t> par_ids = {};
    uint64_t num_records = 0;
    map<parid_t, bool> populated = {}; // partition_id -> populated

    vector<column_t> columns = {};
    mdb::symbol_t symbol;
    std::string tb_name = "";
  };

  std::map<std::string, tb_info_t> tb_infos_;
  std::map<MultiValue, MultiValue> dist2sid_;
  std::map<MultiValue, MultiValue> stock2sid_;
  TxLogServer *tx_sched_;
  Frame* frame_;

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


  void BuildTableInfoPtr();

  void insert_dist_mapping(const MultiValue &mv);

  MultiValue &dist_mapping(const MultiValue &mv);

  void insert_stock_mapping(const MultiValue &mv);

  MultiValue &stock_mapping(const MultiValue &mv);

  int GetPartition(const std::string &tb_name,
                   const MultiValue &key,
                   unsigned int &site_id);

  std::vector<siteid_t> SiteIdsForKey(const std::string &tb_name,
                                      const MultiValue &key);

  int GetTablePartitions(const std::string &tb_name,
                         std::vector<unsigned int> &par_ids);

//  int do_populate_table(const std::vector<std::string> &table_names,
//                        unsigned int sid);
//
//  int do_tpcc_dist_partition_populate_table(
//      const std::vector<std::string> &table_names,
//      unsigned int sid);
//
//  int do_tpcc_real_dist_partition_populate_table(
//      const std::vector<std::string> &table_names,
//      unsigned int sid);

  virtual bool Ready2Populate(tb_info_t *tb_info);

  void release_foreign_values();

  uint32_t PartitionFromKey(const MultiValue &key,
                            const tb_info_t *tb_info);

  uint32_t modulus(const MultiValue &key, uint32_t num_partitions);

  uint32_t int_modulus(const MultiValue &key, uint32_t num_partitions);

  Sharding();
  Sharding(const Sharding& sharding);

 public:
  int init_schema(const std::string &tb_name,
                         mdb::Schema *schema,
                         mdb::symbol_t *symbol);

  int GetTableNames(uint32_t sid,
                    vector<string> &tables);

  int get_number_rows(std::map<std::string, uint64_t> &table_map);


  virtual int PopulateTables(parid_t par_id);
  virtual int PopulateTable(tb_info_t *tb_info, parid_t par_id);
  virtual void PreparePrimaryColumn(tb_info_t *tb_info,
                                    uint32_t col_index,
                                    mdb::Schema::iterator &col_it) = 0;
  virtual bool GenerateRowData(tb_info_t *tb_info,
                       uint32_t &sid,
                       Value &key_value,
                       vector<Value> &row_data) = 0;
  virtual void InsertRow(tb_info_t *tb_info,
                         uint32_t &partition_id,
                         Value &key_value,
                         const mdb::Schema *schema,
                         mdb::Table *const table_ptr,
                         mdb::SortedTable *tbl_sec_ptr);
  virtual void InsertRowData(tb_info_t *tb_info,
                             uint32_t &partition_id,
                             Value &key_value,
                             const mdb::Schema *schema,
                             mdb::Table *const table_ptr,
                             mdb::SortedTable *tbl_sec_ptr,
                             vector<Value> &row_data) = 0;

  virtual ~Sharding();

  friend class Config;
};


} // namespace janus
