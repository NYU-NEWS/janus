#include "all.h"
/*
 * RO-6: Define class members for RO6DTxn in dtxn.hpp
 * For each member, we add RO-6 specific logics, and then call super class's
 * corresponding method
 */
namespace rococo {
    void RO6DTxn::start(
            const RequestHeader &header,
            const std::vector<mdb::Value> &input,
            bool *deferred,
            std::vector<mdb::Value> *output
    ) {
            // For all columns this txn is querying, update each
            // column's rtxnIdTracker

            RCCDTxn::start(header, input, deferred, output);
    }

}