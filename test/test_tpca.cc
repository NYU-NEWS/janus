
//#include "all.h"
//
//
//#include "deptran/all.h"
//
//#define N (10)
//
//using namespace rococo;
//
//
//TEST(tpca, choping) {
//    TxnRequest req;
//    req.txn_type_ = TPCA;
//    req.n_try_ = 1;
//    req.input_ = {Value(1), Value(2), Value(3), Value(4)};
//    req.callback_ = [] (TxnReply& reply) {
//
//    };
//    TpcaPaymentChopper ch;
//    ch.init(req);
//
//    EXPECT_EQ(ch.inputs_.size(), 3);
//    EXPECT_EQ(ch.inputs_[0][0], Value(1));
//    EXPECT_EQ(ch.inputs_[0][1], Value(4));
//}
//
//TEST(tpca, one) {
////    // create schema
////
////    Schema sch1;
////    sch1.append_column(Value::I32, "id", true);
////    sch1.append_column(Value::I32, "branch_id");
////    sch1.append_column(Value::I64, "balance");
////    sch1.sanity_check();
////
////    Table *tbl1 = new Table(sch1);
////    tbl1->insert_row({Value(1), Value(1), Value((int64_t)100)});
////
////    TxnRunner::reg_table("customer", tbl1);
////
////    Schema sch2;
////    sch2.append_column(Value::I32, "id", true);
////    sch2.append_column(Value::I32, "branch_id");
////    sch2.append_column(Value::I64, "balance");
////    sch2.sanity_check();
////
////    Table *tbl2 = new Table(sch2);
////    tbl2->insert_row({Value(1), Value(1), Value((int64_t)100)});
////
////    TxnRunner::reg_table("teller", tbl2);
////
////
////    Schema sch3;
////    sch3.append_column(Value::I32, "id", true);
////    sch3.append_column(Value::I64, "balance");
////    sch3.sanity_check();
////
////    Table *tbl3 = new Table(sch3);
////    tbl3->insert_row({Value(1), Value((int64_t)100)});
////
////    TxnRunner::reg_table("branch", tbl3);
////
////    TpcaPiece tpca_piece;
////    tpca_piece.reg_pieces();
////
////    // init service implement
////    rrr::Service *deptran_service = new DepTranService();
////
////    // init rrr::PollMgr 8 threads
////    struct rrr::poll_options poll_opt;
////    poll_opt.n_threads = 1;
////    rrr::PollMgr *poll_mgr = new rrr::PollMgr(poll_opt);
////
////    // init base::ThreadPool
////    unsigned int num_threads = 1;
////    base::ThreadPool *thread_pool = new base::ThreadPool(num_threads);
////
////    // init rrr::Server
////    rrr::Server *server = new rrr::Server(poll_mgr, thread_pool);
////
////    // reg service
////    server->reg(deptran_service);
////
////    // start rpc server
////    server->start("127.0.0.1:9876");
////
////
////    // start a client, and do one.
////    std::string addr("127.0.0.1:9876");
////    std::vector<std::string> addrs({addr});
////    TpcaPaymentCoordinator coo(1, addrs);
////    uint32_t max_try = 0;
////    coo.do_one(max_try, 1, 1, 1, (int64_t)100L);
////
////    EXPECT_EQ(max_try, 1);
////
////    vector<Value> output;
////    tbl1->read(Value(1), {2}, &output);
////    EXPECT_EQ(output[0], Value((int64_t)200));
////
////    tbl2->read(Value(1), {2}, &output);
////    //Log::debug("%" PRIu64 "", output[0].get_i64());
////    EXPECT_EQ(output[0], Value((int64_t)200));
////
////    tbl3->read(Value(1), {1}, &output);
//////    Log::debug("%" PRIu64 "", output[0].get_i64());
////    EXPECT_EQ(output[0], Value((int64_t)200));
////
////    // let us see what can happen if we write a value that is not there.
////    // coo.do_one(); core dump !!! TODO (shuai) fix, find out what is wrong.
//}
//
