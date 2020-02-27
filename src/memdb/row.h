#pragma once

#include <map>
#include <list>
#include <unordered_map>
#include <vector>
#include <string>
#include <ctime>
#include <boost/crc.hpp>      // for boost::crc_basic, boost::crc_optimal

#include "utils.h"
#include "schema.h"
#include "locking.h"

#include "rrr.hpp"

using std::list;

namespace mdb {

// forward declartion
class Schema;
class Table;


class Row: public RefCounted {
  // fixed size part
  char *fixed_part_;
  int n_columns_ = 0;
  enum {
    DENSE,
    SPARSE,
  };

  int kind_;

  union {
    // for DENSE rows
    struct {
      // var size part
      char *dense_var_part_;

      // index table for var size part (marks the stop of a var segment)
      int *dense_var_idx_;
    };

    // for SPARSE rows
    std::string *sparse_var_;
  };

  Table *tbl_;

//protected:
 public:

  void update_fixed(const Schema::column_info *col, void *ptr, int len);

  bool rdonly_;
  const Schema *schema_;

  // hidden ctor, factory model
  Row() : fixed_part_(nullptr), kind_(DENSE),
          dense_var_part_(nullptr), dense_var_idx_(nullptr),
          tbl_(nullptr), rdonly_(false), schema_(nullptr) { }

  // RefCounted should have protected dtor
  virtual ~Row();

  void copy_into(Row *row) const;

  // generic row creation
  static Row *create(Row *raw_row,
                     const Schema *schema,
                     const std::vector<const Value *> &values);

  // helper function for row creation
  static void fill_values_ptr(const Schema *schema,
                              std::vector<const Value *> &values_ptr,
                              const Value &value,
                              size_t fill_counter) {
    values_ptr[fill_counter] = &value;
  }

  // helper function for row creation
  static void fill_values_ptr(const Schema *schema,
                              std::vector<const Value *> &values_ptr,
                              const std::pair<const std::string, Value> &pair,
                              size_t fill_counter) {
    int col_id = schema->get_column_id(pair.first);
    verify(col_id >= 0);
    values_ptr[col_id] = &pair.second;
  }

 public:

  virtual symbol_t rtti() const {
    return symbol_t::ROW_BASIC;
  }

  const Schema *schema() const {
    return schema_;
  }
  bool readonly() const {
    return rdonly_;
  }
  void make_readonly() {
    rdonly_ = true;
  }
  void make_sparse();
  void set_table(Table *tbl) {
    if (tbl != nullptr) {
      verify(tbl_ == nullptr);
    }
    tbl_ = tbl;
  }
  const Table *get_table() const {
    return tbl_;
  }

  Value get_column(int column_id) const;
  Value get_column(const std::string &col_name) const {
    return get_column(schema_->get_column_id(col_name));
  }
  virtual MultiBlob get_key() const;

  blob get_blob(int column_id) const;
  blob get_blob(const std::string &col_name) const {
    return get_blob(schema_->get_column_id(col_name));
  }

  virtual uint16_t Checksum() {
    auto n = schema_->columns_count();
    unsigned char ret = 0;
    for (auto i = 0; i < n; i++) {
      auto blob = get_blob(i);
      auto c = (const unsigned char*)blob.data;
      for (int j = 0; j < blob.len; j++) {
        ret ^= *c;
        c++;
      }
//      boost::crc_basic<16>  crc_ccitt1( 0x1021, 0xFFFF, 0, false, false );
//      crc_ccitt1.process_bytes(blob.data, blob.len);
//      auto cs = crc_ccitt1.checksum();
//      ret ^= cs;
    }
    return ret;
  }

  void update(int column_id, i32 v) {
    const Schema::column_info *info = schema_->get_column_info(column_id);
    verify(info->type == Value::I32);
    update_fixed(info, &v, sizeof(v));
  }

  void update(int column_id, i64 v) {
    const Schema::column_info *info = schema_->get_column_info(column_id);
    verify(info->type == Value::I64);
    update_fixed(info, &v, sizeof(v));
  }
  void update(int column_id, double v) {
    const Schema::column_info *info = schema_->get_column_info(column_id);
    verify(info->type == Value::DOUBLE);
    update_fixed(info, &v, sizeof(v));
  }
  void update(int column_id, const std::string &str);
  void update(int column_id, const Value &v);

  void update(const std::string &col_name, i32 v) {
    this->update(schema_->get_column_id(col_name), v);
  }
  void update(const std::string &col_name, i64 v) {
    this->update(schema_->get_column_id(col_name), v);
  }
  void update(const std::string &col_name, double v) {
    this->update(schema_->get_column_id(col_name), v);
  }
  void update(const std::string &col_name, const std::string &v) {
    this->update(schema_->get_column_id(col_name), v);
  }
  void update(const std::string &col_name, const Value &v) {
    this->update(schema_->get_column_id(col_name), v);
  }

  // compare based on keys
  // must have same schema!
  int compare(const Row &another) const;

  bool operator==(const Row &o) const {
    return compare(o) == 0;
  }
  bool operator!=(const Row &o) const {
    return compare(o) != 0;
  }
  bool operator<(const Row &o) const {
    return compare(o) == -1;
  }
  bool operator>(const Row &o) const {
    return compare(o) == 1;
  }
  bool operator<=(const Row &o) const {
    return compare(o) != 1;
  }
  bool operator>=(const Row &o) const {
    return compare(o) != -1;
  }

  virtual Row *copy() const {
    Row *row = new Row();
    copy_into(row);
    return row;
  }

  template<class Container>
  static Row *create(const Schema *schema, const Container &values) {
    verify(values.size() == schema->columns_count());
    std::vector<const Value *> values_ptr(values.size(), nullptr);
    size_t fill_counter = 0;
    for (auto it = values.begin(); it != values.end(); ++it) {
      fill_values_ptr(schema, values_ptr, *it, fill_counter);
      fill_counter++;
    }
    return Row::create(new Row(), schema, values_ptr);
  }

  void to_string(std::string &str) {
    size_t s = str.size();
    int len = s;
    len += (sizeof(schema_->fixed_part_size_)
        + schema_->fixed_part_size_
        + sizeof(kind_));
    if (kind_ == DENSE && schema_->var_size_cols_ > 0) {
      len += schema_->var_size_cols_;
      len += dense_var_idx_[schema_->var_size_cols_ - 1];
      str.resize(len);
      int i = s;
      memcpy((void *) (str.data() + i),
             (void *) (&schema_->fixed_part_size_),
             sizeof(schema_->fixed_part_size_));
      i += sizeof(schema_->fixed_part_size_);
      memcpy((void *) (str.data() + i),
             (void *) (fixed_part_),
             schema_->fixed_part_size_);
      i += schema_->fixed_part_size_;
      memcpy((void *) (str.data() + i), (void *) (&kind_), sizeof(kind_));
      i += sizeof(kind_);
      memcpy((void *) (str.data() + i),
             (void *) dense_var_idx_,
             schema_->var_size_cols_);
      i += schema_->var_size_cols_;
      memcpy((void *) (str.data() + i),
             (void *) dense_var_part_,
             dense_var_idx_[schema_->var_size_cols_ - 1]);
      i += dense_var_idx_[schema_->var_size_cols_ - 1];
      verify(i == len);
    } else {
      str.resize(len);
      int i = s;
      memcpy((void *) (str.data() + i),
             (void *) (&schema_->fixed_part_size_),
             sizeof(schema_->fixed_part_size_));
      i += sizeof(schema_->fixed_part_size_);
      memcpy((void *) (str.data() + i),
             (void *) (fixed_part_),
             schema_->fixed_part_size_);
      i += schema_->fixed_part_size_;
      memcpy((void *) (str.data() + i), (void *) (&kind_), sizeof(kind_));
      i += sizeof(kind_);
      verify(i == len);
    }
  }
};


class CoarseLockedRow: public Row {
  RWLock lock_;

 protected:

  CoarseLockedRow() : Row(), lock_() {}
  // protected dtor as required by RefCounted
  ~CoarseLockedRow() { }

  void copy_into(CoarseLockedRow *row) const {
    this->Row::copy_into((Row *) row);
    row->lock_ = lock_;
  }

 public:

  virtual symbol_t rtti() const {
    return symbol_t::ROW_COARSE;
  }

  bool rlock_row_by(lock_owner_t o) {
    return lock_.rlock_by(o);
  }
  bool wlock_row_by(lock_owner_t o) {
    return lock_.wlock_by(o);
  }
  bool unlock_row_by(lock_owner_t o) {
    return lock_.unlock_by(o);
  }

  virtual Row *copy() const {
    CoarseLockedRow *row = new CoarseLockedRow();
    copy_into(row);
    return row;
  }

  template<class Container>
  static CoarseLockedRow *create(const Schema *schema,
                                 const Container &values) {
    verify(values.size() == schema->columns_count());
    std::vector<const Value *> values_ptr(values.size(), nullptr);
    size_t fill_counter = 0;
    for (auto it = values.begin(); it != values.end(); ++it) {
      fill_values_ptr(schema, values_ptr, *it, fill_counter);
      fill_counter++;
    }
    return (CoarseLockedRow *) Row::create(new CoarseLockedRow(),
                                           schema,
                                           values_ptr);
  }
};

/*
class FineLockedRow: public Row {
    RWLock* lock_;
    void init_lock(int n_columns) {
        lock_ = new RWLock[n_columns];
    }

protected:

    // protected dtor as required by RefCounted
    ~FineLockedRow() {
        delete[] lock_;
    }

    void copy_into(FineLockedRow* row) const {
        this->Row::copy_into((Row *) row);
        int n_columns = schema_->columns_count();
        row->init_lock(n_columns);
        for (int i = 0; i < n_columns; i++) {
            row->lock_[i] = lock_[i];
        }
    }

public:

    virtual symbol_t rtti() const {
        return symbol_t::ROW_FINE;
    }

    bool rlock_column_by(column_id_t column_id, lock_owner_t o) {
        return lock_[column_id].rlock_by(o);
    }
    bool rlock_column_by(const std::string& col_name, lock_owner_t o) {
        column_id_t column_id = schema_->get_column_id(col_name);
        return lock_[column_id].rlock_by(o);
    }
    bool wlock_column_by(column_id_t column_id, lock_owner_t o) {
        return lock_[column_id].wlock_by(o);
    }
    bool wlock_column_by(const std::string& col_name, lock_owner_t o) {
        int column_id = schema_->get_column_id(col_name);
        return lock_[column_id].wlock_by(o);
    }
    bool unlock_column_by(column_id_t column_id, lock_owner_t o) {
        return lock_[column_id].unlock_by(o);
    }
    bool unlock_column_by(const std::string& col_name, lock_owner_t o) {
        column_id_t column_id = schema_->get_column_id(col_name);
        return lock_[column_id].unlock_by(o);
    }

    virtual Row* copy() const {
        FineLockedRow* row = new FineLockedRow();
        copy_into(row);
        return row;
    }

    template <class Container>
    static FineLockedRow* create(const Schema* schema, const Container& values) {
        verify(values.size() == schema->columns_count());
        std::vector<const Value*> values_ptr(values.size(), nullptr);
        size_t fill_counter = 0;
        for (auto it = values.begin(); it != values.end(); ++it) {
            fill_values_ptr(schema, values_ptr, *it, fill_counter);
            fill_counter++;
        }
        FineLockedRow* raw_row = new FineLockedRow();
        raw_row->init_lock(schema->columns_count());
        return (FineLockedRow * ) Row::create(raw_row, schema, values_ptr);
    }
};
*/
class FineLockedRow: public Row {
  typedef enum {
    WAIT_DIE,
    WOUND_WAIT,
    TIMEOUT
  } type_2pl_t;
  static type_2pl_t type_2pl_;
  rrr::ALock *lock_;
  //rrr::ALock *lock_;
  void init_lock(int n_columns) {
    //lock_ = new rrr::ALock *[n_columns];
    switch (type_2pl_) {
      case WAIT_DIE: {
        lock_ = new rrr::WaitDieALock[n_columns];
        //rrr::WaitDieALock *locks = new rrr::WaitDieALock[n_columns];
        //for (int i = 0; i < n_columns; i++)
        //    lock_[i] = (locks + i);
        break;
      }
      case WOUND_WAIT: {
        lock_ = new rrr::WoundDieALock[n_columns];
        //rrr::WoundDieALock *locks = new rrr::WoundDieALock[n_columns];
        //for (int i = 0; i < n_columns; i++)
        //    lock_[i] = (locks + i);
        break;
      }
      case TIMEOUT: {
        verify(0);
        lock_ = new rrr::TimeoutALock[n_columns];
        //rrr::TimeoutALock *locks = new rrr::TimeoutALock[n_columns];
        //for (int i = 0; i < n_columns; i++)
        //    lock_[i] = (locks + i);
        break;
      }
      default:
        verify(0);
    }
  }

 protected:

  // protected dtor as required by RefCounted
  ~FineLockedRow() {
    switch (type_2pl_) {
      case WAIT_DIE:
        delete[] ((rrr::WaitDieALock *) lock_);
        //delete[] ((rrr::WaitDieALock *)lock_[0]);
        break;
      case WOUND_WAIT:
        delete[] ((rrr::WoundDieALock *) lock_);
        //delete[] ((rrr::WoundDieALock *)lock_[0]);
        break;
      case TIMEOUT:
        delete[] ((rrr::TimeoutALock *) lock_);
        //delete[] ((rrr::TimeoutALock *)lock_[0]);
        break;
      default:
        verify(0);
    }
    //delete [] lock_;
  }

  //FIXME
  void copy_into(FineLockedRow *row) const {
    verify(0);
    this->Row::copy_into((Row *) row);
    int n_columns = schema_->columns_count();
    row->init_lock(n_columns);
    for (int i = 0; i < n_columns; i++) {
      //row->lock_[i] = lock_[i];
    }
  }

 public:
  static void set_wait_die() {
    type_2pl_ = WAIT_DIE;
  }

  static void set_wound_wait() {
    type_2pl_ = WOUND_WAIT;
  }

  virtual symbol_t rtti() const {
    return symbol_t::ROW_FINE;
  }

  rrr::ALock *get_alock(colid_t column_id) {
    //return lock_[column_id];
    switch (type_2pl_) {
      case WAIT_DIE:
        return ((rrr::WaitDieALock *) lock_) + column_id;
      case WOUND_WAIT:
        return ((rrr::WoundDieALock *) lock_) + column_id;
      case TIMEOUT:
        return ((rrr::TimeoutALock *) lock_) + column_id;
      default:
        verify(0);
    }
  }

  uint64_t reg_wlock(colid_t column_id,
                     std::function<void(uint64_t)> succ_callback,
                     std::function<void(void)> fail_callback);

  uint64_t reg_rlock(colid_t column_id,
                     std::function<void(uint64_t)> succ_callback,
                     std::function<void(void)> fail_callback);

  void abort_lock_req(colid_t column_id, uint64_t lock_req_id);

  void unlock_column_by(colid_t column_id, uint64_t lock_req_id);

  virtual Row *copy() const {
    FineLockedRow *row = new FineLockedRow();
    copy_into(row);
    return row;
  }

  template<class Container>
  static FineLockedRow *create(const Schema *schema, const Container &values) {
    verify(values.size() == schema->columns_count());
    std::vector<const Value *> values_ptr(values.size(), nullptr);
    size_t fill_counter = 0;
    for (auto it = values.begin(); it != values.end(); ++it) {
      fill_values_ptr(schema, values_ptr, *it, fill_counter);
      fill_counter++;
    }
    FineLockedRow *raw_row = new FineLockedRow();
    raw_row->init_lock(schema->columns_count());
    return (FineLockedRow *) Row::create(raw_row, schema, values_ptr);
  }
};


// inherit from CoarseLockedRow since we need locking on commit phase, when doing 2 phase commit
class VersionedRow: public CoarseLockedRow {
 public:
//  version_t *ver_ = nullptr;
  std::vector<version_t> ver_{};
  // only for tapir. TODO: extract
  std::vector<list<version_t>> prepared_rver_{};
  // only for tapir. TODO: extract
  std::vector<list<version_t>> prepared_wver_{};
  void init_ver(int n_columns) {
//    ver_ = new version_t[n_columns];
//    memset(ver_, 0, sizeof(version_t) * n_columns);
    ver_.resize(n_columns, 0);
    prepared_rver_.resize(n_columns, {});
    prepared_wver_.resize(n_columns, {});
  }

  version_t max_prepared_rver(colid_t column_id) {
    if (prepared_wver_[column_id].size() > 0) {
      return prepared_wver_[column_id].back();
    } else {
      return 0;
    }
  }

  version_t min_prepared_wver(colid_t column_id) {
    if (prepared_rver_[column_id].size() > 0) {
      return prepared_rver_[column_id].front();
    } else {
      return 0;
    }
  }

  void insert_prepared_wver(colid_t column_id, version_t ver) {
    prepared_wver_[column_id].push_back(ver);
    prepared_rver_[column_id].sort(); // TODO optimize
  }

  void remove_prepared_wver(colid_t column_id, version_t ver) {
    prepared_wver_[column_id].remove(ver);
  }

  void insert_prepared_rver(colid_t column_id, version_t ver) {
    prepared_rver_[column_id].push_back(ver);
    prepared_rver_[column_id].sort(); // TODO optimize
  }

  void remove_prepared_rver(colid_t column_id, version_t ver) {
    prepared_rver_[column_id].remove(ver);
  }

 protected:

  // protected dtor as required by RefCounted
  ~VersionedRow() {
//    delete[] ver_;
  }

  void copy_into(VersionedRow *row) const {
    this->CoarseLockedRow::copy_into((CoarseLockedRow *) row);
    int n_columns = schema_->columns_count();
    row->init_ver(n_columns);
//    memcpy(row->ver_, this->ver_, n_columns * sizeof(version_t));
    row->ver_ = this->ver_;
    verify(row->ver_.size() > 0);
  }

 public:

  virtual symbol_t rtti() const {
    return symbol_t::ROW_VERSIONED;
  }

  version_t get_column_ver(colid_t column_id) const {
    verify(ver_.size() > 0);
    verify(column_id < ver_.size());
    return ver_[column_id];
  }

  void set_column_ver(colid_t column_id, version_t ver) {
    ver_[column_id] = ver;
  }

  void incr_column_ver(colid_t column_id) {
    ver_[column_id] ++;
  }

  virtual Row *copy() const {
    VersionedRow *row = new VersionedRow();
    copy_into(row);
    return row;
  }


  Value get_column(int column_id) const {
    Value v = Row::get_column(column_id);
    v.ver_ = get_column_ver(column_id);
    return v;
  }

  template<class Container>
  static VersionedRow *create(const Schema *schema, const Container &values) {
    verify(values.size() == schema->columns_count());
    std::vector<const Value *> values_ptr(values.size(), nullptr);
    size_t fill_counter = 0;
    for (auto it = values.begin(); it != values.end(); ++it) {
      fill_values_ptr(schema, values_ptr, *it, fill_counter);
      fill_counter++;
    }
    VersionedRow *raw_row = new VersionedRow();
    raw_row->init_ver(schema->columns_count());
    return (VersionedRow *) Row::create(raw_row, schema, values_ptr);
  }
};

} // namespace mdb
