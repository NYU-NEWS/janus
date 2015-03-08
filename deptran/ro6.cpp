#include "all.h"
#include "memdb/row.h"
/*
 * RO-6: Define class members for RO6DTxn in dtxn.hpp
 * For each member, we add RO-6 specific logics, and then call super class's
 * corresponding method
 */
namespace rococo {

    /*
     * Pseudo code here
     *
    // start phase only used for write txns
    void RO6DTxn::start(
            const RequestHeader &header,
            const std::vector<mdb::Value> &input,
            bool *deferred,
            std::vector<mdb::Value> *output
    ) {
            // For all columns this txn is querying, update each
            // column's rtxnIdTracker
            // call super class's original method

            // TODO: for Shuai cell_map is the information we need.
            // This should not be declared here. Instead, cell_map should be a private member of RO6DTxn class,
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

void RO6DTxn::kiss(mdb::Row* r, int col, bool immediate) {
    RCCDTxn::kiss(r, col, immediate);

    if (!read_only_) {
        // We only query cell's rxn table for non-read txns
        MultiVersionedRow* row = (MultiVersionedRow*) r;
        std::vector<i64> ro_ids = row->rtxn_tracker.getReadTxnIds(col);
        ro_.insert(ro_ids.begin(), ro_ids.end());

    } else {

    }
}

void RO6DTxn::start_ro(
        const RequestHeader &header,
        const std::vector<mdb::Value> &input,
        std::vector<mdb::Value> &output) {
    RCCDTxn::start_ro(header, input, output);
}

} //namespace rococo