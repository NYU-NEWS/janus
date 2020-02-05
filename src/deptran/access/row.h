//
// Created by chance_Lv on 2020/2/5.
//
#pragma once

#include "linked_vector.h"
#include "tx.h"
#include <unordered_map>
/*
 * Defines the row structure in ACCESS.
 * A row is a map of txn_queue (linked_vector); a txn_queue for each col
 */
namespace janus {
    class AccRow {
    public:



    private:
        // a map of txn_q; keys are cols, values are linkedvectors that holding txns (versions)
        std::unordered_map<int, LinkedVector<AccTxnRec>> txn_queue;



    };

}   // namespace janus
