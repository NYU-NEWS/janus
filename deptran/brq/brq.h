#pragma once

#include "all.h"

namespace rococo {

class BRQCommand {
public:
  txntype_t type_;
  std::vector<mdb::Value> *input_;
  std::vector<mdb::Value> *output_;
};

class BRQDTxn {
public:

  ballot_t   ballot_cmd_seen_; // initialized as (0,cmd_id.COO_ID)
  ballot_t   ballot_cmd_vote_; // initialized as NULL
  ballot_t   ballot_deps_seen_; // initialized as (0, 0)
  ballot_t   ballot_deps_vote_; // initialized as NULL
  Command cmd_;             // , initialized as NULL
  // deps// initialized as NULL
  // status, initialized as (PREPARED, FAST_PREPARED, UNKNOWN)
  enum status_t {
    UKN,
    CMT, // committing D_CPTD
//    CPD, // transitively committing
    DCD   // decided
  };
  status_t status_;
  txnid_t txn_id_;

  // TODO should this be abstracted as a command?
  //    typedef struct {
  //        RequestHeader header;
  //        std::vector<mdb::Value> inputs;
  //        row_map_t row_map;
  //    } DeferredRequest;

  // TODO rewrite dependency graph
  BRQGraph *graph_;

  //    std::vector<DeferredRequest> dreqs_;
  //    Vertex <TxnInfo> *tv_;

  //    std::vector<TxnInfo *> conflict_txns_; // This is read-only transaction

  //    bool read_only_;

  BRQDTxn(txnid_t txn_id, DTxnMgr * mgr, bool ro);

  // fast-accept/start
  void fast_accept(FastAcceptRequest &req, FastAcceptReply *rep, rrr::DeferredReply *defer);
  // prepare
  void prepare(PrepareReqeust &request, PrepareReply *rep, rrr::DeferredReply *reply);
  // accept
  void accept(AcceptRequest& request, AcceptReply *reply, rrr::DeferredReply *defer);
  // commit
  void commit(const CommitRequest &req, CommitReply *rep, rrr::DeferredReply *defer);
  void commit_tcpd(CommitReply *rep);
  // inquire
  void inquire(InquiryReply *rep, rrr::DeferredReply *defer);
  void inquire_dcpd(InquiryReply *rep, rrr::DeferredReply *defer);

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

} // namespace rococo
