//#include "txn-chopper-factory.h"
//// TODO remove this.
//#include "all.h"
//
//
//namespace rococo {
//
//TxnChopper *TxnChopperFactory::gen_chopper(TxnRequest &req, int benchmark) {
//  TxnChopper *ch = NULL;
//  switch (benchmark) {
//    case TPCA:
//      verify(req.txn_type_ == TPCA_PAYMENT);
//      ch = new TpcaPaymentChopper();
//      break;
//    case TPCC:
//      ch = new TpccChopper();
//      break;
//    case TPCC_DIST_PART:
//      ch = new TpccDistChopper();
//      break;
//    case TPCC_REAL_DIST_PART:
//      ch = new TpccRealDistChopper();
//      break;
//    case RW_BENCHMARK:
//      ch = new RWChopper();
//      break;
//    case MICRO_BENCH:
//      ch = new MicroBenchChopper();
//      break;
//    default:
//      verify(0);
//  }
//  verify(ch != NULL);
//  ch->sss_ = Config::GetConfig()->sharding_;
//  ch->init(req);
//  return ch;
//}
//
//} // namespace rcc
