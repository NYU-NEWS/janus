//#include <array>
//
//#include "base/all.hpp"
//#include "memdb/schema.h"
//#include "memdb/row.h"
//#include "memdb/table.h"
//#include "memdb/txn.h"
//
//#include "test-helper.h"
//
//using namespace std;
//using namespace mdb;
//using namespace base;
//
//static void benchmark_kv(TxnMgr* mgr, symbol_t table_type, symbol_t row_type) {
//    Schema* schema = new Schema;
//    schema->add_key_column("key", Value::I32);
//    schema->add_column("value", Value::I64);
//
//    Table* table = nullptr;
//    if (table_type == TBL_UNSORTED) {
//        table = new UnsortedTable(schema);
//    } else if (table_type == TBL_SORTED) {
//        table = new SortedTable(schema);
//    } else if (table_type == TBL_SNAPSHOT) {
//        table = new SnapshotTable(schema);
//    } else {
//        verify(0);
//    }
//
//    int n_populate = 100 * 1000;
//    Timer timer;
//    timer.start();
//    const i64 x = 3;
//    for (i32 i = 0; i < n_populate; i++) {
//        array<Value, 2> row_data = { { Value(i), Value(x) } };
//        Row* row = nullptr;
//        if (row_type == ROW_BASIC) {
//            row = Row::create(schema, row_data);
//        } else if (row_type == ROW_COARSE) {
//            row = CoarseLockedRow::create(schema, row_data);
//        } else if (row_type == ROW_FINE) {
//            row = FineLockedRow::create(schema, row_data);
//        } else if (row_type == ROW_VERSIONED) {
//            row = VersionedRow::create(schema, row_data);
//        } else {
//            row = MultiVersionedRow::create(schema, row_data);
//        }
//        table->insert(row);
//    }
//    timer.stop();
//    report_qps("populating table", n_populate, timer.elapsed());
//
//    // do updates
//    Counter txn_counter;
//    const int batch_size = 10 * 1000;
//    int n_batches = 0;
//    timer.start();
//    Rand rnd;
//    i64 tracker = 0;
//    for (;;) {
//        for (int i = 0; i < batch_size; i++) {
//            txn_id_t txnid = txn_counter.next();
//            Txn* txn = mgr->start(txnid);
//            ResultSet rs = txn->query(table, Value(i32(rnd.next(0, n_populate))));
//            while (rs) {
//                Row* row = rs.next();
//                txn->write_column(row, 1, Value(tracker));
//            }
//            txn->commit_or_abort();
//            delete txn;
//            ++tracker;
//        }
//        n_batches++;
//        if (timer.elapsed() > 16.0) {
//            break;
//        }
//    }
//    timer.stop();
//    int n_update = n_batches * batch_size;
//    report_qps("update rows", n_update, timer.elapsed());
//
//    delete table;
//    delete schema;
//}
//
//TEST(benchmark, kv_unsafe_unsorted) {
//    TxnMgrUnsafe mgr;
//    benchmark_kv(&mgr, TBL_UNSORTED, ROW_BASIC);
//}
//
////TEST(benchmark, kv_2pl_unsorted) {
////    TxnMgr2PL mgr;
////    benchmark_kv(&mgr, TBL_UNSORTED, ROW_FINE);
////}
////
////TEST(benchmark, kv_occ_unsorted) {
////    TxnMgrOCC mgr;
////    benchmark_kv(&mgr, TBL_UNSORTED, ROW_VERSIONED);
////}
//
//TEST(benchmark, kv_unsafe_sorted) {
//    TxnMgrUnsafe mgr;
//    benchmark_kv(&mgr, TBL_SORTED, ROW_BASIC);
//}
//
//TEST(benchmark, kv_multiver_sorted) {
//    TxnMgrUnsafe mgr;
//    benchmark_kv(&mgr, TBL_SORTED, ROW_MULTIVER);
//}
//
//TEST(benchmark, kv_multiver_unsorted) {
//    TxnMgrUnsafe mgr;
//    benchmark_kv(&mgr, TBL_UNSORTED, ROW_MULTIVER);
//}
//
////TEST(benchmark, kv_2pl_sorted_coarse) {
////    TxnMgr2PL mgr;
////    benchmark_kv(&mgr, TBL_SORTED, ROW_COARSE);
////}
////
////TEST(benchmark, kv_2pl_sorted_fine) {
////    TxnMgr2PL mgr;
////    benchmark_kv(&mgr, TBL_SORTED, ROW_FINE);
////}
////
////TEST(benchmark, kv_occ_sorted) {
////    TxnMgrOCC mgr;
////    benchmark_kv(&mgr, TBL_SORTED, ROW_VERSIONED);
////}
////
////TEST(benchmark, kv_2pl_snapshot_coarse) {
////    TxnMgr2PL mgr;
////    benchmark_kv(&mgr, TBL_SNAPSHOT, ROW_COARSE);
////}
////
////TEST(benchmark, kv_2pl_snapshot_fine) {
////    TxnMgr2PL mgr;
////    benchmark_kv(&mgr, TBL_SNAPSHOT, ROW_FINE);
////}
//
////TEST(benchmark, kv_occ_snapshot) {
////    TxnMgrOCC mgr;
////    benchmark_kv(&mgr, TBL_SNAPSHOT, ROW_VERSIONED);
////}
