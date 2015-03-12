#pragma once

#include <algorithm>
#include "all.h"

namespace rococo {

#define IS_MODE_RCC (Config::get_config()->get_mode() == MODE_RCC)
#define IS_MODE_RO6 (Config::get_config()->get_mode() == MODE_RO6)
#define IS_MODE_2PL (Config::get_config()->get_mode() == MODE_2PL)
#define IS_MODE_OCC (Config::get_config()->get_mode() == MODE_OCC)

#define IN_PHASE_1 (dtxn->phase_ == 1)
#define TPL_PHASE_1 (output_size == nullptr)

struct entry_t {
    Vertex<TxnInfo> *last_ = NULL; // last transaction(write) that touches this item. (arriving order)

    const entry_t &operator=(const entry_t &rhs) {
        last_ = rhs.last_;
        return *this;
    }

    entry_t() {
    }

    entry_t(const entry_t &o) {
        last_ = o.last_;
    }
};

#include "multi_value.hpp"

typedef std::unordered_map<char *, std::unordered_map<mdb::MultiBlob, mdb::Row *, mdb::MultiBlob::hash> > row_map_t;
//typedef std::unordered_map<cell_locator_t, entry_t *, cell_locator_t_hash> cell_entry_map_t;
// in charge of storing the pre-defined procedures
//

class DTxn;
typedef std::function<void (
        DTxn *txn,
        const RequestHeader& header,
        const Value* input,
        rrr::i32 input_size,
        rrr::i32* res,
        Value* output,
        rrr::i32* output_size,
        row_map_t *row_map)> TxnHandler;

typedef enum {
    DF_REAL,
    DF_NO,
    DF_FAKE
} defer_t;

typedef struct {
    TxnHandler txn_handler;
    defer_t defer;
} txn_handler_defer_pair_t;


/**
* This class holds all the hard-coded transactions pieces.
*/
class TxnRegistry {
public:

    static inline void reg(
            base::i32 t_type, 
            base::i32 p_type,
            defer_t defer, 
            const TxnHandler& txn_handler) {
        auto func_key = std::make_pair(t_type, p_type);
        auto it = all_.find(func_key);
        verify(it == all_.end());
        all_[func_key] = (txn_handler_defer_pair_t){txn_handler, defer};
    }

    static inline txn_handler_defer_pair_t get(
            const base::i32 t_type,
            const base::i32 p_type) {
        auto it = all_.find(std::make_pair(t_type, p_type));
        // Log::debug("t_type: %d, p_type: %d", t_type, p_type);
        verify(it != all_.end());
        return it->second;
    }

    static inline txn_handler_defer_pair_t get(const RequestHeader& req_hdr) {
        return get(req_hdr.t_type, req_hdr.p_type);
    }



private:
    // prevent instance creation
    TxnRegistry() {}
    static map<std::pair<base::i32, base::i32>, txn_handler_defer_pair_t> all_;
//    static map<std::pair<base::i32, base::i32>, LockSetOracle> lck_oracle_;

};

class DTxnMgr;

class DTxn {
public:
    int64_t tid_;
    DTxnMgr *mgr_;
    int phase_;

    DTxn() = delete;

    DTxn(i64 tid, DTxnMgr* mgr) : tid_(tid), mgr_(mgr), phase_(0) {}

    virtual mdb::Row* create(const mdb::Schema* schema, const std::vector<mdb::Value> &values) = 0;

    virtual ~DTxn() {}
};

#include "rcc.hpp"
#include "ro6.hpp"
#include "tpl.hpp"

class DTxnMgr {
public:
    std::map<i64, DTxn*> dtxns_;
    std::map<i64, mdb::Txn *> mdb_txns_;
    mdb::TxnMgr *mdb_txn_mgr_;
    int mode_;

    DTxnMgr(int mode);

    ~DTxnMgr();

    DTxn* create(i64 tid, bool ro=false);

    void destroy(i64 tid) {
        auto it = dtxns_.find(tid);
        verify(it != dtxns_.end());
        delete it->second;
        dtxns_.erase(it);
    }

    DTxn* get(i64 tid) {
        auto it = dtxns_.find(tid);
        verify(it != dtxns_.end());
        return it->second;
    }

    DTxn* get_or_create(i64 tid, bool ro=false) {
        auto it = dtxns_.find(tid);
        if (it == dtxns_.end()) {
            return create(tid, ro);
        } else {
            return it->second;
        }
    }

    inline int get_mode() { return mode_; }


    // Below are function calls that go deeper into the mdb.
    // They are merged from the called TxnRunner.

    inline mdb::Table
    *get_table(const string& name) {
        return mdb_txn_mgr_->get_table(name);
    }

    mdb::Txn *get_mdb_txn(const i64 tid);

    mdb::Txn *get_mdb_txn(const RequestHeader &req);

    mdb::Txn *del_mdb_txn(const i64 tid);

    void get_prepare_log(i64 txn_id,
            const std::vector<i32> &sids,
            std::string *str);


    // TODO: (Shuai: I am not sure this is supposed to be here.)
    // I think it used to initialized the database?
    // So it should be somewhere else?
    void reg_table(const string& name,
            mdb::Table *tbl
    );

    static DTxnMgr* txn_mgr_s;

    static DTxnMgr* get_sole_mgr() {
        verify(txn_mgr_s != NULL);
        return txn_mgr_s;
    }
};

} // namespace rococo
