#ifndef DTXN_H_
#define DTXN_H_

#include "all.h"

namespace rcc {

struct entry_t {
    Vertex<TxnInfo> *last_ = NULL;

//    Vertex<PieInfo> *rsp = nullptr; // read source piece
//    Vertex<PieInfo> *wsp = nullptr;
//    Vertex<TxnInfo> *rst = nullptr;
//    Vertex<TxnInfo> *wst = nullptr;

    const entry_t &operator=(const entry_t &rhs) {
//        rsp = rhs.rsp;
//        wsp = rhs.wsp;
//        rst = rhs.rst;
//        wst = rhs.wst;
        last_ = rhs.last_;
        return *this;
    }

    entry_t() {
//        rsp = nullptr;
//        wsp = nullptr;
//        rst = nullptr;
//        wst = nullptr;
    }

    entry_t(const entry_t &o) {
//       rsp = o.rsp;
//       wsp = o.wsp;
//       rst = o.rst;
//       wst = o.wst;
        last_ = o.last_;
    }

    void touch(Vertex<TxnInfo> *tv, bool immediate);

    // deprecated.
//    void touch(Vertex<PieInfo> *pv, Vertex<TxnInfo> *tv, int target_type) {
//        verify(0);
//
//        if (target_type & OP_W) {
//            int8_t relation = 0;
//            if (rsp != nullptr) {
//                relation = rsp->data_.defer_? DRW : IRW;
//
//                rsp->to_[pv] |= relation;
//                pv->from_[rsp] |= relation;
//
//                rst->to_[tv] |= relation;
//                tv->from_[rst] |= relation;
//            }
//            if (wsp != nullptr) {
//                relation = WW;
//
//                wsp->to_[pv] |= relation;
//                pv->from_[wsp] |= relation;
//
//                wst->to_[tv] |= relation;
//                tv->from_[wst] |= relation;
//            }
//        }
//        if (target_type & OP_R) {
//            int8_t relation = 0;
//            if (target_type & OP_IR) {
//                relation |= WIR;
//            }
//            if (target_type & OP_DR) {
//                relation |= WDR;
//            }
//
//            if (wsp != nullptr) {
//                wsp->to_[pv] |= relation;
//                pv->from_[wsp] |= relation;
//
//                wst->to_[tv] |= relation;
//                tv->from_[wst] |= relation;
//            }
//        }
//
//        if (target_type & OP_W) {
//            wsp = pv;
//            wst = tv;
//            rsp = nullptr;
//            rst = nullptr;
//        } else if (target_type & OP_R) {
//            rsp = pv;
//            rst = tv;
//        }
//    }

    void ro_touch(std::vector<TxnInfo *> *conflict_txns) {
        if (last_)
            conflict_txns->push_back(&last_->data_);
    }
};

class MultiValue {
public:
    MultiValue(): v_(NULL), n_(0) {
    }

    MultiValue(const Value& v): n_(1) {
        v_ = new Value[n_];
        v_[0] = v;
    }
    MultiValue(const vector<Value>& vs): n_(vs.size()) {
        v_ = new Value[n_];
        for (int i = 0; i < n_; i++) {
            v_[i] = vs[i];
        }
    }
    MultiValue(int n): n_(n) {
        v_ = new Value[n_];
    }
    MultiValue(const MultiValue& mv): n_(mv.n_) {
        v_ = new Value[n_];
        for (int i = 0; i < n_; i++) {
            v_[i] = mv.v_[i];
        }
    }
    inline const MultiValue& operator =(const MultiValue& mv) {
        if (&mv != this) {
            if (v_) {
                delete[] v_;
                v_ = NULL;
            }
            n_ = mv.n_;
            v_ = new Value[n_];
            for (int i = 0; i < n_; i++) {
                v_[i] = mv.v_[i];
            }
        }
        return *this;
    }

    bool operator== (const MultiValue &rhs) const {
        if (n_ == rhs.size()) {
            for (int i = 0; i < n_; i++) {
                if (v_[i] != rhs[i]) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    ~MultiValue() {
        if (v_) {
            delete[] v_;
            v_ = NULL;
        }
    }
    int size() const {
        return n_;
    }
    Value& operator[] (int idx) {
        return v_[idx];
    }
    const Value& operator[] (int idx) const {
        return v_[idx];
    }
    int compare(const MultiValue& mv) const;
private:
    Value* v_ = NULL;
    int n_ = 0;
};

inline bool operator <(const MultiValue& mv1, const MultiValue& mv2) {
    return mv1.compare(mv2) == -1;
}

struct cell_locator {
    std::string tbl_name;
    MultiValue primary_key;
    int col_id;

    //std::size_t operator() () const {
    //    return std::hash<std::string>()(tbl_name) ^
    //        (std::hash<std::int>()(col_id) << 1);
    //}

    bool operator== (const cell_locator &rhs) const {
        return (tbl_name == rhs.tbl_name) && (primary_key == rhs.primary_key) && (col_id == rhs.col_id);
    }

    bool operator< (const cell_locator &rhs) const {
        int i = tbl_name.compare(rhs.tbl_name);
        if (i < 0) {
            return true;
        } else if (i > 0) {
            return false;
        } else {
            if (col_id < rhs.col_id) {
                return true;
            } else if (col_id > rhs.col_id) {
                return false;
            } else {
                return primary_key.compare(rhs.primary_key) < 0;
            }
        }
    }
};

struct cell_locator_t {
    char *tbl_name;
    mdb::MultiBlob primary_key;
    int col_id;

    bool operator==(const cell_locator_t &rhs) const {
        return (tbl_name == rhs.tbl_name || 0 == strcmp(tbl_name, rhs.tbl_name)) && (primary_key == rhs.primary_key) && (col_id == rhs.col_id);
    }

    cell_locator_t(char *_tbl_name, int n, int _col_id = 0)
    : tbl_name(_tbl_name), primary_key(n), col_id(_col_id) {
    }
};

struct cell_locator_t_hash {
    size_t operator() (const cell_locator_t &k) const {
        size_t ret = 0;
        ret ^= std::hash<char *>()(k.tbl_name);
        ret ^= mdb::MultiBlob::hash()(k.primary_key);
        ret ^= std::hash<int>()(k.col_id);
        return ret;
    }
};

struct multi_value_hasher{
    size_t operator() (const MultiValue& key) const {
        size_t ret = 0;
        for (int i = 0; i < key.size(); i++) {
            const Value &v = key[i];
            switch (v.get_kind()) {
            case Value::I32:
                ret ^= std::hash<int32_t>() (v.get_i32());
                break;
            case Value::I64:
                ret ^= std::hash<int64_t>() (v.get_i64());
                break;
            case Value::DOUBLE:
                ret ^= std::hash<double>() (v.get_double());
                break;
            case Value::STR:
                ret ^= std::hash<std::string>() (v.get_str());
                break;
            default:
                verify(0);
            }
        }
        return ret;
    }
};

struct cell_locator_hasher{
    size_t operator() (const cell_locator& key) const {
        size_t ret;
        ret = std::hash<std::string>()(key.tbl_name);
        ret <<= 1;
        ret ^= std::hash<int>() (key.col_id);
        ret <<= 1;

        for (int i = 0; i < key.primary_key.size(); i++) {
            const Value &v = key.primary_key[i];
            switch (v.get_kind()) {
            case Value::I32:
                ret ^= std::hash<int32_t>() (v.get_i32());
                break;
            case Value::I64:
                ret ^= std::hash<int64_t>() (v.get_i64());
                break;
            case Value::DOUBLE:
                ret ^= std::hash<double>() (v.get_double());
                break;
            case Value::STR:
                ret ^= std::hash<std::string>() (v.get_str());
                break;
            default:
                verify(0);
            }
        }

        // TODO
        return ret;
    }
};

typedef std::unordered_map<char *, std::unordered_map<mdb::MultiBlob, mdb::Row *, mdb::MultiBlob::hash> > row_map_t;
//typedef std::unordered_map<cell_locator_t, entry_t *, cell_locator_t_hash> cell_entry_map_t;
// in charge of storing the pre-defined procedures
class TxnRegistry {
public:
    typedef std::function<void (const RequestHeader& header,
                                const Value* input,
                                rrr::i32 input_size,
                                rrr::i32* res,
                                Value* output,
                                rrr::i32* output_size,
                                row_map_t *row_map,
//                                cell_entry_map_t *entry_map
                                Vertex<PieInfo> *pv,
                                Vertex<TxnInfo> *tv,
                                std::vector<TxnInfo *> *ro_conflict_txns
                                )> TxnHandler;

    typedef enum {
        DF_REAL,
        DF_NO,
        DF_FAKE
    } defer_t;

    typedef struct {
        TxnHandler txn_handler;
        defer_t defer;
    } txn_handler_defer_pair_t;

//    typedef std::function<void (const RequestHeader& header,
//                                const Value* input,
//                                rrr::i32 input_size,
//                                std::unordered_map<cell_locator_t, int, cell_locator_t_hash> *opset)> LockSetOracle;

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

//    static inline void reg_lock_oracle(base::i32 t_type, base::i32 p_type, const LockSetOracle& lck_oracle) {
//        auto func_key = std::make_pair(t_type, p_type);
//        auto it = lck_oracle_.find(func_key);
//        verify(it == lck_oracle_.end());
//        lck_oracle_[func_key] = lck_oracle;
//    }

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

//    static inline LockSetOracle get_lock_oracle(base::i32 t_type, base::i32 p_type) {
//        auto it = lck_oracle_.find(std::make_pair(t_type, p_type));
//        // Log::debug("t_type: %d, p_type: %d", t_type, p_type);
//        verify(it != lck_oracle_.end());
//        verify(it->second != nullptr);
//        return it->second;
//    }
//
//    static inline LockSetOracle get_lock_oracle(const RequestHeader& req_hdr) {
//        return get_lock_oracle(req_hdr.t_type, req_hdr.p_type);
//    }

    static void pre_execute_2pl(const RequestHeader& header,
                               const std::vector<mdb::Value>& input,
                               rrr::i32* res,
                               std::vector<mdb::Value>* output,
                               DragonBall *db);

    static void pre_execute_2pl(const RequestHeader& header,
                               const Value *input,
                               rrr::i32 input_size,
                               rrr::i32* res,
                               mdb::Value* output,
                               rrr::i32* output_size,
                               DragonBall *db);

    static inline void execute(const RequestHeader& header,
                               const std::vector<mdb::Value>& input,
                               rrr::i32* res,
                               std::vector<mdb::Value>* output) {
        rrr::i32 output_size = output->size();
        get(header).txn_handler(header, input.data(), input.size(),
				res, output->data(), &output_size,
				NULL, NULL, NULL, NULL);
        output->resize(output_size);
    }

    static inline void execute(const RequestHeader& header,
                               const Value *input,
                               rrr::i32 input_size,
                               rrr::i32* res,
                               mdb::Value* output,
                               rrr::i32* output_size) {
        get(header).txn_handler(header, input, input_size,
				res, output, output_size,
				NULL, NULL, NULL, NULL);
    }

    static void exe_deptran_start(
            const RequestHeader &header,
            const std::vector<mdb::Value> &input,
            bool &is_defered,
            std::vector<mdb::Value> &output,
            Vertex<PieInfo> *pv,
            Vertex<TxnInfo> *tv
            //cell_entry_map_t *rw_entry
            );

    static void exe_deptran_start_ro(
            const RequestHeader &header,
            const std::vector<mdb::Value> &input,
            std::vector<mdb::Value> &output,
            std::vector<TxnInfo *> *conflict_txns
            );

    static void exe_deptran_finish(i64 tid,
        std::vector<std::pair<RequestHeader, std::vector<mdb::Value> > > &outputs);

    typedef struct {
        RequestHeader header;
        std::vector<mdb::Value> inputs;
        row_map_t row_map;
    } DeferredRequest;

private:
    // prevent instance creation
    TxnRegistry() {}
    static map<std::pair<base::i32, base::i32>, txn_handler_defer_pair_t> all_;
//    static map<std::pair<base::i32, base::i32>, LockSetOracle> lck_oracle_;
    static map<i64, std::vector<DeferredRequest>> deferred_reqs_;

};

// in charge of locks and staging area
class TxnRunner {
public:

    static void get_prepare_log(i64 txn_id,
            const std::vector<i32> &sids,
            std::string *str);

    static int do_prepare(i64 txn_id);
    static int do_commit(i64 txn_id);
    static int do_abort(i64 txn_id/*, rrr::DeferredReply* defer*/);
    static void init(int mode);
    // finalize, free up resource
    static void fini();
    static inline int get_running_mode() { return running_mode_s; }

    static void reg_table(const string& name,
            mdb::Table *tbl
            );

    static mdb::Txn *get_txn(const RequestHeader &req);

//    static txn_entry_t *get_txn_entry(const RequestHeader &req);
//
//    static PieceStatus *get_piece_status(const RequestHeader &req, bool insert = false);
//
    static inline
        mdb::Table
        *get_table(const string& name) {
        return txn_mgr_s->get_table(name);
    }

    static std::function<void(void)> get_2pl_proceed_callback(
            const RequestHeader &header,
            const mdb::Value *input,
            rrr::i32 input_size,
            rrr::i32 *res);

    static std::function<void(void)> get_2pl_fail_callback(
            const RequestHeader &header,
            rrr::i32 *res,
            mdb::Txn2PL::PieceStatus *ps);

    static std::function<void(void)> get_2pl_succ_callback(
        const RequestHeader &header,
        const mdb::Value *input,
        rrr::i32 input_size,
        rrr::i32 *res,
        mdb::Txn2PL::PieceStatus *ps,
        std::function<void(
            const RequestHeader &,
            const Value *,
            rrr::i32,
            rrr::i32 *)> func);

    static std::function<void(void)> get_2pl_succ_callback(
            const RequestHeader &req,
            const mdb::Value *input,
            rrr::i32 input_size,
            rrr::i32 *res,
            mdb::Txn2PL::PieceStatus *ps);

private:
    // prevent instance creation
    TxnRunner() {}

    static int running_mode_s;

    static map<i64, mdb::Txn *> txn_map_s;
    //static pthread_mutex_t txn_map_mutex_s;
    static mdb::TxnMgr *txn_mgr_s;
};


} // namespace deptran

#endif
