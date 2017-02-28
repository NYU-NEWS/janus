//
// Created by shuai on 2/26/17.
//

#pragma once
#include "__dep__.h"
#include "dtxn.h"

using namespace std;

namespace janus {

class Db;
class Table;

class AbstractKvDb {
 public:
  virtual bool Insert(string& key, mdb::Value& val) = 0;
  virtual bool Remove(string& key) = 0;
  virtual void Put(string& key, mdb::Value& val) = 0;
  virtual mdb::Value&& Get(string& key) = 0;
  virtual bool TransInsert(DTxn& dtxn, string& key, mdb::Value& val) = 0;
  virtual bool TransRemove(DTxn& dtxn, string& key) = 0;
  virtual void TransPut(DTxn& dtxn, string& key, mdb::Value& val) = 0;
  virtual mdb::Value&& TransGet(DTxn& dtxn, string& key) = 0;
};

class SimpleKvDb : public AbstractKvDb {
  typedef std::pair<Value*, void*> val_box_t;
  map<string, val_box_t> sorted_index_{};
  Value empty_value_{0};

  virtual bool Insert(string& key, mdb::Value& val) override {
    Value* v = new Value();
    *v = val;
    auto val_box = std::make_pair(v, nullptr);
    auto ret_pair = sorted_index_.insert(std::make_pair(key, val_box));
    bool ret = ret_pair.second;
    if (!ret) {
      delete v;
    }
    return ret;
  }

  virtual bool Remove(string& key) override {
    auto it = sorted_index_.find(key);
    if (it == sorted_index_.end()) {
      return false;
    } else {
      sorted_index_.erase(it);
      return true;
    }
  };

  virtual void Put(string& key, mdb::Value& val) override {
    Value* v = new Value();
    *v = val;
    auto val_box = std::make_pair(v, nullptr);
    sorted_index_[key] = val_box_t(v, nullptr);
  }

  virtual mdb::Value& Get(string& key) override {
    auto it = sorted_index_.find(key);
    if (it == sorted_index_.end()) {
      return empty_value_;
    } else {
      return *(it->second.first);
    }
  }

  virtual bool TransInsert(DTxn& dtxn, string& key, mdb::Value& val) override{
    verify(0);
  }

  virtual bool TransRemove(DTxn& dtxn, string& key) override {
    verify(0);
  }

  virtual void TransPut(DTxn& dtxn, string& key, mdb::Value& val) override {
    verify(0);
  }

  virtual mdb::Value& TransGet(DTxn& dtxn, string& key) override {
    verify(0);
  }
};

}
