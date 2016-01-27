#pragma once

#include "memdb/utils.h"
#include "memdb/table.h"
#include "memdb/value.h"
#include "memdb/blob.h"
#include "deptran/multi_value.h"
#include "deptran/marshal-value.h"

namespace mdcc {
  using namespace std;
  using mdb::MultiBlob;
  using mdb::version_t;
  using rococo::MultiValue;

  struct Option {
    mdb::column_id_t col_id=-1;
    mdb::Value value;
    version_t version=-1;
    Option() {}
    Option(const Option&) = default;
    Option(mdb::column_id_t col_id, const mdb::Value& value, version_t version) :
        col_id(col_id), value(value), version(version) {}
  };

  class OptionSet {
  protected:
    std::string table_;
    MultiValue key_;
    std::vector<Option> options_;
    bool accepted_ = false;
  public:
    OptionSet(string& table, MultiValue& key, bool accepted) :
        table_(table), key_(key), accepted_(accepted) {}
    OptionSet(std::string table, MultiValue key) : OptionSet(table, key, false) {}
    OptionSet() : OptionSet("", MultiValue()) {}

    void Add(const Option& o) { options_.push_back(o); }
    std::string Table() const { return table_; }
    const MultiValue& Key() const { return key_; }
    const std::vector<Option>& Options() const { return options_; }
    bool Accepted() const { return accepted_ == true; }
    void Accept() { accepted_ = true; }

    friend rrr::Marshal& operator <<(rrr::Marshal& m, const OptionSet& o);
    friend rrr::Marshal& operator >>(rrr::Marshal& m, OptionSet& o);
  };

  typedef std::map<std::pair<string, size_t>, OptionSet*> RecordOptionMap;

  inline rrr::Marshal& operator <<(rrr::Marshal& m, const Option& o) {
    m << o.col_id;
    m << o.version;
    m << o.value;
    return m;
  }

  inline rrr::Marshal& operator <<(rrr::Marshal& m, Option& o) {
    m.read(&o.col_id,sizeof(o.col_id));
    m >> o.col_id;
    m >> o.version;
    m >> o.value;
    return m;
  }

  inline rrr::Marshal& operator >>(rrr::Marshal& m, Option& o) {
    m >> o.col_id;
    m >> o.version;
    m >> o.value;
    return m;
  }

  inline rrr::Marshal& operator <<(rrr::Marshal& m, const OptionSet& o) {
    m << o.table_;
    m << o.options_;
    m << (o.accepted_ ? 1 : 0);
    m << o.key_;
    return m;
  }

  inline rrr::Marshal& operator >>(rrr::Marshal& m, OptionSet& o) {
    m >> o.table_;
    m >> o.options_;
    int i;
    m >> i;
    o.accepted_ = (i) ? true : false;
    m >> o.key_;
    return m;
  }
}