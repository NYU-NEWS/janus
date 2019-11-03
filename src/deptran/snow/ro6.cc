#include "ro6.h"
#include "memdb/row.h"
/*
 * RO-6: Define class members for TxSnow in dtxn.hpp
 * For each member, we add RO-6 specific logics, and then call super class's
 * corresponding method
 */
namespace janus {

/*
 * Pseudo code here
 *
// start phase only used for write txns
void TxSnow::start(
        const RequestHeader &header,
        const std::vector<mdb::Value> &input,
        bool *deferred,
        std::vector<mdb::Value> *output
) {
        // For all columns this txn is querying, update each
        // column's rtxnIdTracker
        // call super class's original method

        // TODO: for Shuai cell_map is the information we need.
        // This should not be declared here. Instead, cell_map should be a private member of TxSnow class,
        // we can just reference it here. I declare it here just in order to show its structure.
        // It's a vector of pairs, for each pair, first element is a pointer to a row with type
        // mdb::MultiVersionedRow, the second element is the column id this txn is querying for that row.
        std::vector<std::pair<mdb::MultiVersionedRow*, i64> > cell_map;
        std::vector<i64> recordedRxnIds;
        for (auto itr : cell_map) {
            mdb::MultiVersionedRow* row = itr.first;
            i64 column_id = itr.second;
            std::vector<i64> txnIds = row->rtxn_tracker.getReadTxnIds(column_id);
            put txnIds into recordedRxnIds
        }

        do whatever orginal rococo start phase does

        then put recordedRxnIds into callback message which is passed back to coordinator
}

// start phase for read only txn
void start_ro(
        const RequestHeader &header,
        const std::vector<mdb::Value> &input,
        std::vector<mdb::Value> &output
) {
    //TODO: for Shuai. get all conflicting ongoing txns (those txns which are writing to the same cell
    //TODO: as this read only transaction is), since this read only txn need to wait
    //TODO: them to commit before proceeds.
    //TODO: so I need an interface to get those ongoing txns.
    1. get a list of conflicting txns
    //TODO: The rest is for Haonan
    3. wait for conflicting write txns to commit
    4. check rtxnIdTracker, and update it
    5. commit this piece of read
}

// TODO: commit phase for write txns
void commit(
        const ChopFinishRequest &req,
        ChopFinishResponse* res,
        rrr::DeferredReply* defer
) {
    1. extract the list of read txn ids in the msg from coordinator
    2. get the columns this txn is going to write
    3. for all those columns, update their rtxnIdTracker
    4. proceeds as orginal rococo
}

*/
//
//void TxSnow::start(
//    const RequestHeader &header,
//    const std::vector<mdb::Value> &input,
//    bool *deferred,
//    ChopStartResponse *res) {
////  RccDTxn::start(header, input, deferred, res);
//
//  std::vector<i64> ro;
//  ro.insert(ro.end(), ro_.begin(), ro_.end());
////  Compressor::vector_to_string(ro, &res->read_only);
////    auto& ro_list = res->ro_list;
//
////    ro_list.insert(ro_list.end(), ro_.begin(), ro_.end());
//
////    ro_list.resize(1);
////    Log::debug("read only tx list size %d carried in start_ack.", ro_.size());
//}

void TxSnow::kiss(mdb::Row *r, int col, bool immediate) {
  verify(0);
//  TxRococo::kiss(r, col, immediate);

  if (!read_only_) {
    // We only query cell's rxn table for non-read txns
    auto row = (RO6Row *) r;
    std::vector<i64> ro_ids = row->rtxn_tracker.getReadTxnIds(col);
    ro_.insert(ro_ids.begin(), ro_ids.end());

    // put the row and col_id into a map. When this txn gets to
    // commit phase, we will need the row and col_id to put ro_list
    // into the table
    // TODO: for Shuai, is it okay to add this in kiss??
//        row_col_map[r] = col;
    // for haonan, i think it is fine this way.
    row_col_map.insert(std::make_pair(r, col));
  }
}

void TxSnow::start_ro(const SimpleCommand &cmd,
                       map<int32_t, Value> &output,
                       rrr::DeferredReply *defer) {
//    RCCDTxn::start_ro(header, input, output, defer);

//  conflict_txns_.clear();
//  auto txn_handler_pair = txn_reg_->get(cmd.root_type_, cmd.type_);
//  int output_size = 300;
//  int res;
//  phase_ = 1;
//  // TODO fix
//  txn_handler_pair.txn_handler(nullptr,
//                               this,
//                               const_cast<SimpleCommand&>(cmd),
//                               &res,
//                               output);
//  // get conflicting transactions
//  std::vector<TxnInfo *> &conflict_txns = conflict_txns_;
//
//  // TODO callback: read the value and return.
//  std::function<void(void)> cb = [&cmd, &output, defer, this]() {
//    int res;
//    int output_size = 0;
//    this->phase_ = 2;
//    auto txn_handler_pair = txn_reg_->get(cmd.root_type_, cmd.type_);
//    // TODO fix
//    txn_handler_pair.txn_handler(nullptr,
//                                 this,
//                                 const_cast<SimpleCommand&>(cmd),
//                                 &res,
//                                 output);
//    defer->reply();
//  };
//  // wait for them become commit.
//
//  DragonBall *ball = new DragonBall(conflict_txns.size() + 1, cb);
//
//  for (auto tinfo: conflict_txns) {
//    tinfo->register_event(TXN_DCD, ball);
//  }
//  ball->trigger();

  // TODO: for Shuai, this does everything read transactions need in
  // start phase. See the comments to its declaration in dtxn.hpp
  // It needs txn_id, row, and column_id for this txn, please implement the
  // interface for that.
  // This function also returns the value for this read, since read txn returns
  // value in start phase (no commit phase). So please also handle the return type
  // of start_ro
  // comment it out for now for compiling
  /*Value result = do_ro(txn_id, &row, col_id);*/
}

//void TxSnow::commit(
//    const ChopFinishRequest &req,
//    ChopFinishResponse *res,
//    rrr::DeferredReply *defer) {
//  std::vector<i64> ids;
//  Compressor::string_to_vector(req.read_only, &ids);
//
//  // handle ro list, put ro ids into table
//  // I assume one txn may query multiple rows on this node?
//  for (auto &pair : row_col_map) {
//    auto row = (RO6Row *) pair.first;
//    int col_id = pair.second;
//    // get current version of the cell this txn is going to update
//    version_t current_version = row->getCurrentVersion(col_id);
//    for (i64 &ro_id : ids) {
//      row->rtxn_tracker.checkIfTxnIdBeenRecorded(col_id, ro_id, true, current_version);
//    }
//  }
//  // We need to commit this txn after updating the table, because we need to know what the
//  // old version number was before committing current version.
//  RccDTxn::commit(req, res, defer);
//}

bool TxSnow::read_column(mdb::Row *r, mdb::colid_t col_id, Value *value) {

  if (read_only_) {
//        if (false) {
    // TODO (for haonan) Hi, can you put the read-only transaction here to make things more clear?
    RO6Row *row = (RO6Row *) r;
    *value = row->get_column(col_id, tid_);
  } else {
    *value = r->get_column(col_id);
  }
  // always allowed
  return true;
}

// TODO (for haonan) is this read only for read_only transaction or all transactions?
// I assume this is only for the read_only transaction, am I correct?
// Please see the above read_column funtion, check whether I am using it correctly.
Value TxSnow::do_ro(i64 txn_id, RO6Row *row, int col_id) {
  Value ret_value = row->get_column(col_id, txn_id);
  return ret_value;
}

} //namespace janus
