#include "__dep__.h"
#include "frame.h"
#include "config.h"
#include "bench/tpcc_real_dist/sharding.h"
#include "rcc/rcc_row.h"
#include "ro6/ro6_row.h"
#include "marshal-value.h"
#include "coordinator.h"
#include "dtxn.h"
#include "scheduler.h"
#include "none/coord.h"
#include "rcc/rcc_coord.h"
#include "ro6/ro6_coord.h"
#include "tpl/coord.h"
#include "occ/coord.h"


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

#include "txn-chopper-factory.h"

#include "tpl/sched.h"


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

Sharding* Frame::CreateSharding(Sharding *sd) {
  Sharding* ret = CreateSharding();
  *ret = *sd;
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

Coordinator* Frame::CreateCoord(cooid_t coo_id, vector<std::string>& servers,
                                int benchmark, int mode,
                                ClientControlServiceImpl *ccsi,
                                uint32_t id, bool batch_start) {
//   TODO
  Coordinator *coo;
  auto attr = this;
  switch (mode) {
    case MODE_2PL:
      coo = new TPLCoord(coo_id, servers, benchmark,
                         mode, ccsi, id, batch_start);
      break;
    case MODE_OCC:
    case MODE_RPC_NULL:
      coo = new OCCCoord(coo_id, servers,
                             benchmark, mode,
                             ccsi, id, batch_start);
      break;
    case MODE_RCC:
      coo = new RCCCoord(coo_id, servers,
                          benchmark, mode,
                          ccsi, id, batch_start);
      break;
    case MODE_RO6:
      coo = new RO6Coord(coo_id, servers,
                          benchmark, mode,
                          ccsi, id, batch_start);
      break;
    case MODE_NONE:
      coo = new NoneCoord(coo_id, servers,
                           benchmark, mode,
                           ccsi, id,
                           batch_start);
      break;
    default:
      verify(0);
  }
  return coo;
}

void Frame::GetTxnTypes(std::map<int32_t, std::string> &txn_types) {
  auto benchmark_ = Config::config_s->benchmark_;
  switch (benchmark_) {
    case TPCA:
      txn_types[TPCA_PAYMENT] = std::string(TPCA_PAYMENT_NAME);
      break;
    case TPCC:
    case TPCC_DIST_PART:
    case TPCC_REAL_DIST_PART:
      txn_types[TPCC_NEW_ORDER] = std::string(TPCC_NEW_ORDER_NAME);
      txn_types[TPCC_PAYMENT] = std::string(TPCC_PAYMENT_NAME);
      txn_types[TPCC_STOCK_LEVEL] = std::string(TPCC_STOCK_LEVEL_NAME);
      txn_types[TPCC_DELIVERY] = std::string(TPCC_DELIVERY_NAME);
      txn_types[TPCC_ORDER_STATUS] = std::string(TPCC_ORDER_STATUS_NAME);
      break;
    case RW_BENCHMARK:
      txn_types[RW_BENCHMARK_W_TXN] = std::string(RW_BENCHMARK_W_TXN_NAME);
      txn_types[RW_BENCHMARK_R_TXN] = std::string(RW_BENCHMARK_R_TXN_NAME);
      break;
    case MICRO_BENCH:
      txn_types[MICRO_BENCH_R] = std::string(MICRO_BENCH_R_NAME);
      txn_types[MICRO_BENCH_W] = std::string(MICRO_BENCH_W_NAME);
      break;
    default:
      Log_fatal("benchmark not implemented");
      verify(0);
  }
}

TxnChopper* Frame::CreateChopper(TxnRequest &req) {
  auto benchmark = Config::config_s->benchmark_;
  TxnChopper *ch = NULL;
  switch (benchmark) {
    case TPCA:
      verify(req.txn_type_ == TPCA_PAYMENT);
      ch = new TpcaPaymentChopper();
      break;
    case TPCC:
      ch = new TpccChopper();
      break;
    case TPCC_DIST_PART:
      ch = new TpccDistChopper();
      break;
    case TPCC_REAL_DIST_PART:
      ch = new TpccRealDistChopper();
      break;
    case RW_BENCHMARK:
      ch = new RWChopper();
      break;
    case MICRO_BENCH:
      ch = new MicroBenchChopper();
      break;
    default:
      verify(0);
  }
  verify(ch != NULL);
  ch->sss_ = Config::GetConfig()->sharding_;
  ch->init(req);
  return ch;
}


DTxn* Frame::CreateDTxn(txnid_t tid, bool ro, Scheduler * mgr) {
  DTxn *ret = nullptr;
  auto mode_ = Config::GetConfig()->mode_;

  switch (mode_) {
    case MODE_2PL:
      ret = new TPLDTxn(tid, mgr);
      verify(ret->mdb_txn_->rtti() == mdb::symbol_t::TXN_2PL);
      break;
    case MODE_OCC:
      ret = new TPLDTxn(tid, mgr);
      break;
    case MODE_NONE:
      ret = new TPLDTxn(tid, mgr);
      break;
    case MODE_RCC:
      ret = new RCCDTxn(tid, mgr, ro);
      break;
    case MODE_RO6:
      ret = new RO6DTxn(tid, mgr, ro);
      break;
    default:
      verify(0);
  }
  return ret;
}

Scheduler * Frame::CreateScheduler() {
  auto mode = Config::GetConfig()->mode_;
  Scheduler *sch = nullptr;
  switch(mode) {
    case MODE_2PL:
      sch = new TPLSched();
      break;
    case MODE_NONE:
    case MODE_RPC_NULL:
    case MODE_OCC:
    case MODE_RCC:
    case MODE_RO6:
      break;
    default:
      sch = new Scheduler(mode);
  }
  return sch;
}

} // namespace rococo;