#pragma once
#include "dep_graph.hpp"
#include "all.h"

namespace rococo {

struct entry_t {
    Vertex<TxnInfo> *last_ = NULL;

    const entry_t &operator=(const entry_t &rhs) {
        last_ = rhs.last_;
        return *this;
    }

    entry_t() {
    }

    entry_t(const entry_t &o) {
        last_ = o.last_;
    }

    void touch(Vertex<TxnInfo> *tv, bool immediate);

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
//
    typedef std::function<void (
            const RequestHeader& header,
            const Value* input,
            rrr::i32 input_size,
            rrr::i32* res,
            Value* output,
            rrr::i32* output_size,
            row_map_t *row_map,
//            cell_entry_map_t *entry_map
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


private:
    // prevent instance creation
    TxnRegistry() {}
    static map<std::pair<base::i32, base::i32>, txn_handler_defer_pair_t> all_;
//    static map<std::pair<base::i32, base::i32>, LockSetOracle> lck_oracle_;

};

class RCC {
public:

    static void start_pie(
            const RequestHeader &header,
            const std::vector<mdb::Value> &input,
            bool *deferred,
            std::vector<mdb::Value> *output
    );

    static void start(
            const RequestHeader &header,
            const std::vector<mdb::Value> &input,
            bool &is_defered,
            std::vector<mdb::Value> &output,
            Vertex<PieInfo> *pv,
            Vertex<TxnInfo> *tv
            //cell_entry_map_t *rw_entry
    );

    static void start_ro(
            const RequestHeader &header,
            const std::vector<mdb::Value> &input,
            std::vector<mdb::Value> &output,
            std::vector<TxnInfo *> *conflict_txns
    );

    static void finish(
            i64 tid,
            std::vector<std::pair<RequestHeader, std::vector<mdb::Value> > > &outputs);

    typedef struct {
        RequestHeader header;
        std::vector<mdb::Value> inputs;
        row_map_t row_map;
    } DeferredRequest;


    static map<i64, std::vector<DeferredRequest>> deferred_reqs_;

    static DepGraph *dep_s;
};

class TPL {
public:
    static int do_prepare(i64 txn_id);

    static int do_commit(i64 txn_id);

    static int do_abort(i64 txn_id/*, rrr::DeferredReply* defer*/);

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
};

// TODO: seems that this class is both in charge of of the Txn playground and the 2PL/OCC controller.
// in charge of locks and staging area
class TxnRunner {
public:

    static void get_prepare_log(i64 txn_id,
            const std::vector<i32> &sids,
            std::string *str);

    static void init(int mode);
    // finalize, free up resource
    static void fini();
    static inline int get_running_mode() { return running_mode_s; }

    static void reg_table(const string& name,
            mdb::Table *tbl
            );

    static mdb::Txn *get_txn(const i64 tid);

    static mdb::Txn *get_txn(const RequestHeader &req);

    static mdb::Txn *del_txn(const i64 tid);

    static inline
        mdb::Table
        *get_table(const string& name) {
        return txn_mgr_s->get_table(name);
    }


private:
    // prevent instance creation
    TxnRunner() {}

    static int running_mode_s;

    static map<i64, mdb::Txn *> txn_map_s;
    static mdb::TxnMgr *txn_mgr_s;

};


} // namespace rococo
