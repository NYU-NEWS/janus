//#ifndef CLIENT_H_
//#define CLIENT_H_
//
//#include "all.h"
//
//
//class Scheduler;
//
//class Client {
//public:
//    uint32_t cli_id_;
//    uint64_t next_txn_id_;
//    Scheduler *sch_;
//
//    Client(uint32_t cli_id) {
//        this->cli_id_ = cli_id;
//        this->next_txn_id_ = cli_id;
//        this->next_txn_id_ <<= 32;
//    }
//
//    uint64_t nextTxnId() {
//        return this->next_txn_id_++;
//    }
//
//    void doOneTpca() {
//        // random A, B, C,
//        // A: 1 ~ 10000
//        // B: 1 ~ 100
//        // C: 1
//        int a = rand() % 10000;
//        int b = rand() % 100;
//        int c = rand() % 1;
//        TxnInput input;
//        input.txn_type = TPCA;
//        input.txn_id_ = nextTxnId();
//        input.putInt("a", a);
//        input.putInt("b", b);
//        input.putInt("c", c);
//
//        sch_->txnRequest(input);
//    }
//};
//
//#endif // CLIENT_H_
