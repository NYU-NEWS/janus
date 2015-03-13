#pragma once

//namespace rococo {
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

    std::vector<TxnInfo *> conflict_txns_; // This is read-only transaction

    bool read_only_;

    RCCDTxn(i64 tid, DTxnMgr *mgr, bool ro) : DTxn(tid, mgr) {
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
            std::vector<mdb::Value> &output,
            rrr::DeferredReply *defer
    );

    virtual void commit(
            const ChopFinishRequest &req,
            ChopFinishResponse *res,
            rrr::DeferredReply *defer
    );

    void to_decide(
            Vertex<TxnInfo> *v,
            rrr::DeferredReply *defer
    );

    void exe_deferred(
            std::vector<std::pair<RequestHeader, std::vector<mdb::Value> > > &outputs
    );

    void send_ask_req(
            Vertex<TxnInfo> *av
    );

    virtual mdb::Row *create(
            const mdb::Schema *schema,
            const std::vector<mdb::Value> &values) {
        return RCCRow::create(schema, values);
    }

    // ??? TODO (Shuai) I do not understand why de-virtual, is it because the return type thing?
    // TODO (Haonan.reply) Yes, that's my understanding... If this function has different prototype from
    // TODO its parent class's function. Can they be virtual functions?
    // TODO (Shuai.reply) I think no.
    // de-virtual this function, since we are going to have different function signature anyway
    // because we need to either pass in a reference or let it return a value -- list of rxn ids
    virtual void kiss(mdb::Row *r, int col, bool immediate);
};

//} // namespace rococo