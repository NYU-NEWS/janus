//
// Created by lamont on 1/4/16.
//
#pragma once

#include "memdb/utils.h"
#include "memdb/table.h"
#include "memdb/value.h"
#include "memdb/blob.h"

namespace mdcc {
  using mdb::MultiBlob;
  using mdb::version_t;

  // options for updates performed by mdcc transactions
  class Option {
  protected:
    const mdb::Table& table_;
    const MultiBlob& key_;
    const mdb::Value& value_;
    const version_t version_;
  public:
    Option() = delete;
    Option(const mdb::Table& table, const MultiBlob& key, const mdb::Value& value, version_t version) :
        table_(table), key_(key), value_(value), version_(version) {
    }

    const mdb::Table& Table() {
      return table_;
    }

    const MultiBlob& Key() {
      return key_;
    }

    const mdb::Value& Value() {
      return value_;
    }

    version_t Version() {
      return version_;
    }
  };
}