//#include "base/all.hpp"
//#include "deptran/all.h"
//
//using namespace rrr;
//using namespace rococo;
//
//TEST(dtxn, txn_registry) {
//    TxnRegistry::reg(1, 1, DF_NO, [] (const RequestHeader& header,
//                               const Value *input,
//                               rrr::i32 input_size,
//                               rrr::i32 *res,
//                               Value *output,
//                               rrr::i32 *output_size,
//                               row_map_t *row_map,
//                               Vertex<PieInfo> *pv,
//                               Vertex<TxnInfo> *tv,
//                               std::vector<TxnInfo *> *conflict_txns
//                               //cell_entry_map_t *entry_map
//                               ) {
//        Log::info("Hello from TxnRegistry");
//    });
//
//    RequestHeader header;
//    std::vector<mdb::Value> input;
//    i32 res;
//    std::vector<mdb::Value> output;
//
//    header.t_type = 1;
//    header.p_type = 1;
//
//    TxnRegistry::execute(header, input, &res, &output);
//}
//
//TEST(dtxn, multi_value) {
//    MultiValue mv(4);
//    mv[2] = Value("hi");
//    EXPECT_EQ(mv.size(), 4);
//}
