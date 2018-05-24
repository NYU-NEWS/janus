//
// Created by shuai on 2/26/17.
//

#pragma once
#include "__dep__.h"
#include "tx.h"

using namespace std;

#define HINT_PLAIN     (0x01)
#define HINT_TRANS     (0x02)

namespace janus {

class Db;

/**
 * This is the "external" key-value db interface
 */
class DbInterface {
 public:
  virtual bool Insert(string& key, const Value& val, int32_t hint) = 0;
  virtual bool Delete(string& key, int32_t hint) = 0;
  virtual void Put(string& key, const Value& val, int32_t hint) = 0;
  virtual Value& Get(string& key, int32_t hint) = 0;
};

/**
 * external relational db interface
 */
class RInterface : public DbInterface {
 public:

  class RTable {
   public:
    RInterface* rdb_{nullptr};
    string name_{};
    RTable(RInterface* rdb, string& name) : rdb_(rdb), name_(name) {}
  };

  class RRow {
   public:
    RTable* rtbl_{nullptr};
    string key_{};
    RRow(RTable* rtbl, string& key): rtbl_(rtbl), key_(key) {}
  };

  DbInterface* db_{nullptr};

  RInterface(DbInterface* db): db_{db}{}

  RTable GetTable(string name) {
    return RTable(this, name);
  }

  RRow GetRow(RTable& tbl, string& key) {
    return RRow(&tbl, key);
  }

  bool InsertRow(string& tbl,
                 string& key,
                 vector<Value> &vals,
                 int32_t hint=HINT_TRANS) {
    // XXX perhaps validate scheme here.
    string k = tbl+key;
    if(!db_->Insert(k, Value(0), hint))
      return false;
    for (int32_t i = 0; i < vals.size(); i++) {
      string k = tbl + key + to_string(i);
      db_->Put(k, vals[i], hint);
    }
    return true;
  }

  bool DeleteRow(string& tbl,
                 string& key,
                 int32_t hint=HINT_TRANS) {
    string k = tbl+key;
    return db_->Delete(k, hint);
  }

  Value& ReadColumn(string& tbl,
                    string& key,
                    colid_t col_id,
                    int32_t hint=HINT_TRANS) {
    string k = tbl+key+std::to_string(col_id);
    return db_->Get(k, hint);
  }

  void WriteColumn(string& tbl,
                   string& key,
                   colid_t col_id,
                   const Value& val,
                   int32_t hint=HINT_TRANS) {
    string k = tbl+key+std::to_string(col_id);
    return db_->Put(k, val, hint);
  }

  Value& ReadColumn(RTable& tbl,
                    string& key,
                    colid_t col_id,
                    int32_t hint=HINT_TRANS) {
    return ReadColumn(tbl.name_, key, col_id, hint);
  }

  void WriteColumn(RTable& tbl,
                   string& key,
                   colid_t col_id,
                   const Value& val,
                   int32_t hint=HINT_TRANS) {
    return WriteColumn(tbl.name_, key, col_id, val, hint);
  }
};

///**
// * This is the "internal" key-value db.
// */
//class LightInnerDb{
//  typedef std::pair<Value*, void*> val_box_t;
//  map<string, val_box_t> sorted_index_{};
//  Value empty_value_{0};
//
//  virtual void* CreateControlUnit() {
//    return nullptr;
//  }
//
//  virtual void DestroyControlUnit(void* ptr) {
//    // do nothing.
//  }
//
//  virtual bool Insert(string& key, mdb::Value& val) override {
//    Value* v = new Value();
//    *v = val;
//    auto val_box = std::make_pair(v, CreateControlUnit());
//    auto ret_pair = sorted_index_.insert(std::make_pair(key, val_box));
//    bool ret = ret_pair.second;
//    if (!ret) {
//      delete v;
//    }
//    return ret;
//  }
//
//  virtual bool Remove(string& key) override {
//    auto it = sorted_index_.find(key);
//    if (it == sorted_index_.end()) {
//      return false;
//    } else {
//      sorted_index_.erase(it);
//      return true;
//    }
//  };
//
//  virtual void Put(string& key, mdb::Value& val) override {
//    Value* v = new Value();
//    *v = val;
//    auto val_box = std::make_pair(v, nullptr);
//    sorted_index_[key] = val_box_t(v, CreateControlUnit());
//  }
//
//  virtual mdb::Value& Get(string& key) override {
//    auto it = sorted_index_.find(key);
//    if (it == sorted_index_.end()) {
//      return empty_value_;
//    } else {
//      return *(it->second.first);
//    }
//  }
//
//  virtual bool TransInsert(DTxn& dtxn, string& key, mdb::Value& val) override{
//    verify(0);
//  }
//
//  virtual bool TransRemove(DTxn& dtxn, string& key) override {
//    verify(0);
//  }
//
//  virtual void TransPut(DTxn& dtxn, string& key, mdb::Value& val) override {
//    verify(0);
//  }
//
//  virtual mdb::Value& TransGet(DTxn& dtxn, string& key) override {
//    verify(0);
//  }
//};

}
