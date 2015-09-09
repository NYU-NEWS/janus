


#include "all.h"

namespace rococo {


//TEST(sm, simple) {
//    TxnInfo ti1;
//    ti1.txn_id_ = 1;
//
//    int i = 0;
//
//    ti1.register_event(TXN_FINISH, [&i] () {
//        i = 2;
//    });
//
//    ti1.register_event(TXN_COMMIT, [&i] () {
//        i = 3;
//    });
//
//    ti1.union_status(TXN_START);
//    EXPECT_EQ(i, 0);
//
//    ti1.union_status(TXN_FINISH);
//    EXPECT_EQ(i, 2); 
//    
//    ti1.union_status(TXN_COMMIT);
//    EXPECT_EQ(i, 3);
//
//    TxnInfo ti2;
//    ti2.txn_id_ = 2;
//    i = 0;
//
//    ti2.register_event(TXN_FINISH, [&i] () {
//        i = 2;
//    });
//
//    ti2.union_status(TXN_COMMIT);
//    EXPECT_EQ(i, 2);
//
//
//
//    TxnInfo ti3;
//    ti3.txn_id_ = 3;
//    ti2.union_status(TXN_COMMIT);
//
//    i = 0;
//
//    ti2.register_event(TXN_COMMIT, [&i] () {
//        i = 2;
//    });
//
//    EXPECT_EQ(i, 2);
//}

} // namespace deptran
