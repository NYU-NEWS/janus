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

    DTxn(i64 tid, DTxnMgr* mgr) : tid_(tid), mgr_(mgr), phase_(0) {

    }

    virtual mdb::Row* create(const mdb::Schema* schema, const std::vector<mdb::Value> &values) = 0;

    virtual ~DTxn() {}
};


class RCCDTxn : public DTxn {
public:

    typedef struct {
        RequestHeader header;
        std::vector<mdb::Value> inputs;
        row_map_t row_map;
    } DeferredRequest;

    static DepGraph *dep_s;

    std::vector<DeferredRequest> dreqs_;
    Vertex<TxnInfo> *tv_;

    std::vector<TxnInfo*> conflict_txns_; // This is read-only transaction

    bool read_only_;

    RCCDTxn(i64 tid, DTxnMgr* mgr, bool ro) : DTxn(tid, mgr) {
        read_only_ = ro;
    }

    virtual void start(
            const RequestHeader &header,
            const std::vector<mdb::Value> &input,
            bool *deferred,
            ChopStartResponse *res
    );

    virtual void start_ro(
            const RequestHeader &header,
            const std::vector<mdb::Value> &input,
            std::vector<mdb::Value> &output
    );

    virtual void commit(
            const ChopFinishRequest &req,
            ChopFinishResponse* res,
            rrr::DeferredReply* defer
    );

    void to_decide(
            Vertex<TxnInfo> *v,
            rrr::DeferredReply* defer
    );

    void exe_deferred(
            std::vector<std::pair<RequestHeader, std::vector<mdb::Value> > > &outputs
    );

    void send_ask_req(
            Vertex<TxnInfo>* av
    );

    virtual mdb::Row* create(
            const mdb::Schema* schema,
            const std::vector<mdb::Value>& values) {
        return RCCRow::create(schema, values);
    }

    // ??? TODO (Shuai) I do not understand why de-virtual, is it because the return type thing?
    // TODO (Haonan.reply) Yes, that's my understanding... If this function has different prototype from
    // TODO its parent class's function. Can they be virtual functions?
    // TODO (Shuai.reply) I think no.
    // de-virtual this function, since we are going to have different function signature anyway
    // because we need to either pass in a reference or let it return a value -- list of rxn ids
    virtual void kiss(mdb::Row* r, int col, bool immediate);
};

class RO6DTxn : public RCCDTxn {
private:
    i64 txnId = tid_;
    std::set<i64> ro_;
    // for remembering row and col for this txn in start phase. row and col will be
    // used by this txn in commit phase.
    std::map<mdb::Row*, int>row_col_map;
public:
    RO6DTxn(i64 tid, DTxnMgr* mgr, bool ro): RCCDTxn(tid, mgr, ro) {
    }

    // Implementing create method
    mdb::Row* create(const mdb::Schema* schema, const std::vector<mdb::Value>& values) {
        return MultiVersionedRow::create(schema, values);
    }


    virtual void start(
            const RequestHeader &header,
            const std::vector<mdb::Value> &input,
            bool *deferred,
            ChopStartResponse *res
    ) {
        RCCDTxn::start(header, input, deferred, res);
        res->ro_list.insert(res->ro_list.end(), ro_.begin(), ro_.end());
    }

    virtual void start_ro(
            const RequestHeader &header,
            const std::vector<mdb::Value> &input,
            std::vector<mdb::Value> &output
    );

    // the start function above and this commit function only for general(write) transactions
    virtual void commit(
            const ChopFinishRequest &req,
            ChopFinishResponse* res,
            rrr::DeferredReply* defer
    ) {
        const std::vector<i64> &ro_list = req.ro_list;
        // handle ro list, put ro ids into table
        // I assume one txn may query multiple rows on this node?
        for (std::pair<mdb::Row*, int> entry : row_col_map) {
            mdb::Row* row = entry.first;
            int col_id = entry.second;
            MultiVersionedRow* r = (MultiVersionedRow*) row;
            // get current version of the cell this txn is going to update
            version_t current_version = r->getCurrentVersion(col_id);
            for (i64 ro_id : ro_list) {
                r->rtxn_tracker.checkIfTxnIdBeenRecorded(col_id, ro_id, true, current_version);
            }
        }
        // We need to commit this txn after updating the table, because we need to know what the
        // old version number was before committing current version.
        RCCDTxn::commit(req, res, defer);
    }

    // This is not called by a read-only-transaction's start phase,
    virtual void kiss(
            mdb::Row* r,
            int col, bool immediate
    );

    // Called by ro_start. It contains the main logics for ROT's start phase
    // For instance, check txnid table to see if this txn's id is in the table.
    // If it is, then return old version accordingly; if not, add its id into the table
    // *but, before doing those, wait for all conflicting write txns commit.
    // It also does the read, and returns the value with correct version.
    Value do_ro(i64 txn_id, MultiVersionedRow* row, int col_id);
};

class TPLDTxn : public DTxn {
public:
    mdb::Txn* mdb_txn_;

    TPLDTxn(i64 tid, DTxnMgr* mgr);

    int prepare();

    int commit();

    int abort();

    // This method should not be used for now.
    mdb::Row* create(const mdb::Schema* schema, const std::vector<mdb::Value>& values) {
        verify(0);
        return nullptr;
    }

    std::function<void(void)> get_2pl_proceed_callback(
            const RequestHeader &header,
            const mdb::Value *input,
            rrr::i32 input_size,
            rrr::i32 *res
    );

    std::function<void(void)> get_2pl_fail_callback(
            const RequestHeader &header,
            rrr::i32 *res,
            mdb::Txn2PL::PieceStatus *ps
    );

    std::function<void(void)> get_2pl_succ_callback(
            const RequestHeader &header,
            const mdb::Value *input,
            rrr::i32 input_size,
            rrr::i32 *res,
            mdb::Txn2PL::PieceStatus *ps,
            std::function<void(
                    const RequestHeader &,
                    const Value *,
                    rrr::i32,
                    rrr::i32 *)> func
    );

    std::function<void(void)> get_2pl_succ_callback(
            const RequestHeader &req,
            const mdb::Value *input,
            rrr::i32 input_size,
            rrr::i32 *res,
            mdb::Txn2PL::PieceStatus *ps
    );

    // Below are merged from TxnRegistry.
    void pre_execute_2pl(
            const RequestHeader& header,
            const std::vector<mdb::Value>& input,
            rrr::i32* res,
            std::vector<mdb::Value>* output,
            DragonBall *db
    );


    void pre_execute_2pl(
            const RequestHeader& header,
            const Value *input,
            rrr::i32 input_size,
            rrr::i32* res,
            mdb::Value* output,
            rrr::i32* output_size,
            DragonBall *db
    );

    void execute(
            const RequestHeader& header,
            const std::vector<mdb::Value>& input,
            rrr::i32* res,
            std::vector<mdb::Value>* output
    ) {
        rrr::i32 output_size = output->size();
        TxnRegistry::get(header).txn_handler(nullptr, header, input.data(), input.size(),
                res, output->data(), &output_size,
                NULL);
        output->resize(output_size);
    }

    inline void execute(
            const RequestHeader& header,
            const Value *input,
            rrr::i32 input_size,
            rrr::i32* res,
            mdb::Value* output,
            rrr::i32* output_size
    ) {
        TxnRegistry::get(header).txn_handler(nullptr, header, input, input_size,
                res, output, output_size,
                NULL);
    }
};

// TODO: seems that this class is both in charge of of the Txn playground and the 2PL/OCC controller.
// in charge of locks and staging area
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

    /**
    * This is legecy code to keep minimal changes on old codes.
    */

    static DTxnMgr* txn_mgr_s;

    static DTxnMgr* get_sole_mgr() {
        verify(txn_mgr_s != NULL);
        return txn_mgr_s;
    }

};


} // namespace rococo
