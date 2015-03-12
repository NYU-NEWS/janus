#pragma once


class RO6DTxn : public RCCDTxn {
private:
    i64 txnId = tid_;
    std::set<i64> ro_;
    // for remembering row and col for this txn in start phase. row and col will be
    // used by this txn in commit phase.
    // for haonan, I think it should be like this?
    std::set<std::pair<mdb::Row*, int>> row_col_map;
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
        for (auto &pair : row_col_map) {
            auto row = (RO6Row*)pair.first;
            int col_id = pair.second;
            // get current version of the cell this txn is going to update
            version_t current_version = row->getCurrentVersion(col_id);
            for (i64 ro_id : ro_list) {
                row->rtxn_tracker.checkIfTxnIdBeenRecorded(col_id, ro_id, true, current_version);
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