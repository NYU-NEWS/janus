#pragma once

// namespace rococo {

#define ballot_t uint64_t
#define cooid_t uint32_t
#define txnid_t uint64_t
#define txntype_t uint32_t

class BRQCommand {
public:

  txntype_t type_;
  std::vector<mdb::Value> *input_;
  std::vector<mdb::Value> *output_;
};

class BRQDTxn : public DTxn {
public:

  ballot_t   ballot_cmd_seen_; // initialized as (0,cmd_id.COO_ID)
  ballot_t   ballot_cmd_vote_; // initialized as NULL
  ballot_t   ballot_dep_seen_; // initialized as (0, 0)
  ballot_t   ballot_dep_vote_; // initialized as NULL
  BRQCommand cmd_;             // , initialized as NULL
  // deps// initialized as NULL
  // status, initialized as (PREPARED, FAST_PREPARED, UNKNOWN)
  BRQStatus status_;

  // TODO should this be abstracted as a command?
  //    typedef struct {
  //        RequestHeader header;
  //        std::vector<mdb::Value> inputs;
  //        row_map_t row_map;
  //    } DeferredRequest;

  // TODO rewrite dependency graph
  static BRQGraph *dep_s;

  //    std::vector<DeferredRequest> dreqs_;
  //    Vertex <TxnInfo> *tv_;

  //    std::vector<TxnInfo *> conflict_txns_; // This is read-only transaction

  //    bool read_only_;

  BRQDTxn(i64 tid, DTxnMgr * mgr, bool ro);

  // fast-accept/start
  void fast_accept();
  // prepare
  void prepare();
  // accept
  void accept();
  // commit
  void commit();
  // inquire
  void inquire();

  //    virtual void start(
  //            const RequestHeader &header,
  //            const std::vector<mdb::Value> &input,
  //            bool *deferred,
  //            ChopStartResponse *res
  //    );
  //
  //    virtual void commit(
  //            const ChopFinishRequest &req,
  //            ChopFinishResponse *res,
  //            rrr::DeferredReply *defer
  //    );
  //
  //    void to_decide(
  //            Vertex <TxnInfo> *v,
  //            rrr::DeferredReply *defer
  //    );
  //
  //    void exe_deferred(
  //            std::vector<
  //                std::pair<RequestHeader,
  //                          std::vector<mdb::Value>
  //                         >
  //            > &outputs
  //    );
  //
  //    void send_ask_req(
  //            Vertex <TxnInfo> *av
  //    );
  //
  //    virtual mdb::Row *create(
  //            const mdb::Schema *schema,
  //            const std::vector<mdb::Value> &values) {
  //        return RCCRow::create(schema, values);
  //    }
  //
  //    virtual void kiss(
  //            mdb::Row *r,
  //            int col,
  //            bool immediate
  //    );

  //    virtual bool read_column(
  //            mdb::Row *row,
  //            mdb::column_id_t col_id,
  //            Value *value
  //    );
  //
  //    virtual bool write_column(
  //            mdb::Row* row,
  //            mdb::column_id_t col_id,
  //            const Value& value
  //    );
  //
  //    virtual bool insert_row(
  //            mdb::Table* tbl,
  //            mdb::Row* row
  //    );
  //
  //    virtual bool remove_row(
  //            mdb::Table* tbl,
  //            mdb::Row* row
  //    );
};

// } // namespace rococo
