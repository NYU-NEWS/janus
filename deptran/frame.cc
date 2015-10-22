#include "__dep__.h"
#include "frame.h"
#include "config.h"
#include "bench/tpcc_real_dist/sharding.h"
#include "rcc/rcc_row.h"
#include "ro6/ro6_row.h"

namespace rococo {

Sharding* Frame::CreateSharding() {

  Sharding* ret;
  auto bench = Config::config_s->benchmark_;
  switch (bench) {
    case TPCC_REAL_DIST_PART:
      ret = new TPCCDSharding();
      break;
    default:
      verify(0);
  }

  return ret;
}

mdb::Row* Frame::CreateRow(const mdb::Schema *schema,
                           vector<Value> &row_data) {
  auto mode = Config::GetConfig()->mode_;
  mdb::Row* r = nullptr;
  switch (mode) {
    case MODE_2PL:
      r = mdb::FineLockedRow::create(schema, row_data);
      break;

    case MODE_NONE: // FIXME
    case MODE_OCC:
      r = mdb::VersionedRow::create(schema, row_data);
      break;

    case MODE_RCC:
      r = RCCRow::create(schema, row_data);
      break;

    case MODE_RO6:
      r = RO6Row::create(schema, row_data);
      break;

    default:
      verify(0);
  }
  return r;
}


} // namespace rococo;