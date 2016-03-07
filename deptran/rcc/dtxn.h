#pragma once
#include "../dtxn.h"

#define PHASE_RCC_START (1)
#define PHASE_RCC_COMMIT (2)

namespace rococo {
class RccDTxn: public DTxn {
 public:

  DepGraph *dep_s;

  vector<SimpleCommand> dreqs_;
  Vertex <TxnInfo> *tv_;

  std::vector<TxnInfo *> conflict_txns_; // This is read-only transaction

  bool read_only_;

  RccDTxn(
      i64 tid,
      Scheduler *mgr,
      bool ro
  );

  virtual void StartLaunch(
      const SimpleCommand& cmd,
      ChopStartResponse *res,
      rrr::DeferredReply *defer
  );

  virtual void StartAfterLog(const SimpleCommand& cmd,
                             ChopStartResponse *res,
                             rrr::DeferredReply *defer
  );

  virtual bool start_exe_itfr(
      defer_t defer,
      TxnHandler &handler,
      const SimpleCommand& cmd,
      map<int32_t, Value> *output
  );

  // TODO remove
  virtual void start(
      const RequestHeader &header,
      const std::vector <mdb::Value> &input,
      bool *deferred,
      ChopStartResponse *res
  );

  virtual void start_ro(const SimpleCommand&,
                        map<int32_t, Value> &output,
                        rrr::DeferredReply *defer);

  virtual void commit(
      const ChopFinishRequest &req,
      ChopFinishResponse *res,
      rrr::DeferredReply *defer
  );

  virtual void commit_anc_finish(
      Vertex <TxnInfo> *v,
      rrr::DeferredReply *defer
  );

  virtual void commit_scc_anc_commit(
      Vertex <TxnInfo> *v,
      rrr::DeferredReply *defer
  );

  void to_decide(
      Vertex <TxnInfo> *v,
      rrr::DeferredReply *defer
  );

  void exe_deferred(
      std::vector <std::pair<RequestHeader,
                             map<int32_t, Value> > > &outputs
  );

  void send_ask_req(
      Vertex <TxnInfo> *av
  );


  void inquire(
      CollectFinishResponse *res,
      rrr::DeferredReply *defer
  );

  virtual mdb::Row *CreateRow(
      const mdb::Schema *schema,
      const std::vector<mdb::Value> &values) {
    return RCCRow::create(schema, values);
  }

  virtual void kiss(
      mdb::Row *r,
      int col,
      bool immediate
  );



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
