#include <vector>

#include "rrr.hpp"
#include "deptran/all.h"

//using namespace base;
//using namespace rpc;
//using namespace std;
//using namespace deptran;
//
//
//TEST(bench, basic) {
//    // start the server
//
//    // get site id
//    int benchmark = Config::get_config()->get_benchmark();
//    unsigned int sid = Config::get_config()->get_site_id();
//
//    // get all tables
//    std::vector<std::string> table_names;
//    EXPECT_GT(Sharding::get_table_names(sid, table_names), 0);
//
//    // set running mode
//    TxnRunner::init(Config::get_config()->get_mode());
//
//    // init table and reg in TxnRunner
//    std::vector<std::string>::iterator table_it = table_names.begin();
//    for (; table_it != table_names.end(); table_it++) {
//        mdb::Schema *schema = new mdb::Schema();
//        mdb::symbol_t symbol;
//        Sharding::init_schema(*table_it, schema, &symbol);
//        mdb::Table *tb;
//        switch(symbol) {
//            case mdb::TBL_SORTED:
//                tb = new mdb::SortedTable(schema);
//                break;
//            case mdb::TBL_UNSORTED:
//                tb = new mdb::UnsortedTable(schema);
//                break;
//            case mdb::TBL_SNAPSHOT:
//                tb = new mdb::SnapshotTable(schema);
//                break;
//            default:
//                verify(0);
//        }
//        TxnRunner::reg_table(*table_it, tb);
//    }
//    Sharding::populate_table(table_names, sid);
//
//    // register piece
//    Piece *piece = Piece::get_piece(Config::get_config()->get_benchmark());
//    piece->reg_all();
//
//    // benchmark!
//    DepTranService* deptran = new DepTranServiceImpl;
//    base::Timer timer;
//    timer.start();
//    int benchmark_sec = 10;
//    uint32_t coo_id_ = 0;
//    uint64_t txn_id_ = 0;
//    uint64_t pie_id_ = 0;
//
//    while (timer.elapsed() < benchmark_sec) {
//        // pre-process
//        TxnRequest req;
//        TxnRequestFactory::init_txn_req(&req, coo_id_);
//        TxnChopper *ch = TxnChopperFactory::gen_chopper(req, benchmark);
//
//        RequestHeader header;
//        header.cid = coo_id_;
//        header.tid = txn_id_;
//        header.t_type = ch->txn_type_;
//
//        int pi;
//        int32_t p_type;
//        std::vector<Value> *input;
//        int32_t server_id;
//        rrr::i32 deptran_res = 0;
//        int res;
//        int output_size;
//        while ((res = ch->next_piece(input, output_size, server_id, pi, p_type)) == 0) {
//            header.pid = pie_id_++;
//            header.p_type = p_type;
//
//            std::vector<Value> deptran_output;
//            deptran->start_pie(header, *input, output_size, &deptran_res, &deptran_output);
//
//            EXPECT_EQ(SUCCESS, deptran_res);
//
//            ch->start_callback(pi, deptran_res, deptran_output);
//        }
//        if (Config::get_config()->get_mode() == MODE_OCC) {
//            deptran->prepare_txn(header.tid, &deptran_res);
//            deptran->commit_txn(header.tid, &deptran_res);
//        }
//
//        delete ch;
//
//        if (txn_id_ % 1000 == 0) {
//            Log::info("%u transactions done, avg qps = %d", txn_id_, (int) (txn_id_ / (timer.elapsed())));
//        }
//
//        txn_id_++;
//    }
//
//    Log::info("%d seconds passed, stop benchmark", benchmark_sec);
//
//    // free resources
//    TxnRunner::fini();
//}
//
//
//TEST(bench, basic_with_marshal) {
//    // start the server
//
//    // get site id
//    int benchmark = Config::get_config()->get_benchmark();
//    unsigned int sid = Config::get_config()->get_site_id();
//
//    // get all tables
//    std::vector<std::string> table_names;
//    EXPECT_GT(Sharding::get_table_names(sid, table_names), 0);
//
//    // set running mode
//    TxnRunner::init(Config::get_config()->get_mode());
//
//    // init table and reg in TxnRunner
//    std::vector<std::string>::iterator table_it = table_names.begin();
//    for (; table_it != table_names.end(); table_it++) {
//        mdb::Schema *schema = new mdb::Schema();
//        mdb::symbol_t symbol;
//        Sharding::init_schema(*table_it, schema, &symbol);
//        mdb::Table *tb;
//        switch(symbol) {
//            case mdb::TBL_SORTED:
//                tb = new mdb::SortedTable(schema);
//                break;
//            case mdb::TBL_UNSORTED:
//                tb = new mdb::UnsortedTable(schema);
//                break;
//            case mdb::TBL_SNAPSHOT:
//                tb = new mdb::SnapshotTable(schema);
//                break;
//            default:
//                verify(0);
//        }
//        TxnRunner::reg_table(*table_it, tb);
//    }
//    Sharding::populate_table(table_names, sid);
//
//    // register piece
//    Piece *piece = Piece::get_piece(Config::get_config()->get_benchmark());
//    piece->reg_all();
//
//    // benchmark!
//    DepTranService* deptran = new DepTranServiceImpl;
//    base::Timer timer;
//    timer.start();
//    int benchmark_sec = 10;
//    uint32_t coo_id_ = 0;
//    uint64_t txn_id_ = 0;
//    uint64_t pie_id_ = 0;
//
//    while (timer.elapsed() < benchmark_sec) {
//        // pre-process
//        TxnRequest req;
//        TxnRequestFactory::init_txn_req(&req, coo_id_);
//        TxnChopper *ch = TxnChopperFactory::gen_chopper(req, benchmark);
//
//        RequestHeader header;
//        header.cid = coo_id_;
//        header.tid = txn_id_;
//        header.t_type = ch->txn_type_;
//
//        int pi;
//        int32_t p_type;
//        std::vector<Value> *input;
//        int32_t server_id;
//        rrr::i32 deptran_res = 0;
//        int res;
//        int output_size;
//        while ((res = ch->next_piece(input, output_size, server_id, pi, p_type)) == 0) {
//            header.pid = pie_id_++;
//            header.p_type = p_type;
//
//            std::vector<Value> deptran_output;
//
//            Marshal m;
//            m << header;
//
//            deptran->start_pie(header, *input, output_size, &deptran_res, &deptran_output);
//
//            EXPECT_EQ(SUCCESS, deptran_res);
//
//            ch->start_callback(pi, deptran_res, deptran_output);
//            m << deptran_output;
//        }
//        if (Config::get_config()->get_mode() == MODE_OCC) {
//            deptran->prepare_txn(header.tid, &deptran_res);
//            deptran->commit_txn(header.tid, &deptran_res);
//        }
//
//        delete ch;
//
//        if (txn_id_ % 1000 == 0) {
//            Log::info("%u transactions done, avg qps = %d", txn_id_, (int) (txn_id_ / (timer.elapsed())));
//        }
//
//        txn_id_++;
//    }
//
//    Log::info("%d seconds passed, stop benchmark", benchmark_sec);
//
//    // free resources
//    TxnRunner::fini();
//}
//
//
//TEST(bench, thread_sync) {
//    ThreadPool* thrpool = new ThreadPool(2);
//
//    struct sync_stat {
//        Counter counter;
//        Mutex m;
//        CondVar cv;
//        bool done = false;
//    } s_stat;
//
//    static const long n_total_ops = 100 * 1000;
//
//    base::Timer timer;
//    timer.start();
//
//    for (int i = 0; i < n_total_ops; i++) {
//        s_stat.counter.next();
//        s_stat.m.lock();
//        s_stat.cv.signal();
//        s_stat.m.unlock();
//    }
//
//    struct DecrWorker {
//        int decr_runs;
//        struct sync_stat& s_stat;
//        ThreadPool* thrpool;
//
//        DecrWorker(struct sync_stat& s_stat, ThreadPool* thrpool): decr_runs(0), s_stat(s_stat), thrpool(thrpool) {}
//
//        void operator() () {
//            //Log::info("callede");
//            while (s_stat.counter.peek_next() <= 0) {
//                s_stat.m.lock();
//                s_stat.cv.wait(s_stat.m);
//                s_stat.m.unlock();
//            }
//            s_stat.counter.next(-1);
//            decr_runs++;
//            if (decr_runs < n_total_ops) {
//
//                thrpool->run_async([this] {
//                    (*this)();
//                });
//            } else {
//                s_stat.done = true;
//            }
//        }
//    };
//
//    DecrWorker* dw = new DecrWorker(s_stat, thrpool);
//
//    thrpool->run_async([dw] {
//        (*dw)();
//    });
//
//    while (!s_stat.done) {
//        usleep(50);
//    }
//
//    EXPECT_EQ(s_stat.counter.peek_next(), 0);
//
//    thrpool->release();
//
//    Log::info("thread sync = %ld/s", long(n_total_ops / timer.elapsed()));
//}
