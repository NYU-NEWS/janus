#include "../rcc/row.h"
#include "ro6_row.h"

namespace janus {

// RO-6: initialize static variable
version_t RO6Row::ver_s = 0;


void RO6Row::copy_into(RO6Row *row) const {
  RccRow::copy_into((RccRow *) row);
  // TODO copy contents for RO6Row data structure.
}

/*
 * RO-6: Do garbage collection. First we record current system time and put it into time_segment
 * since we only call this function when processing x100th value.
 * Then, check which values are deprecated, remove them from old_values_, and also update
 * time_segment
 *
 * @param COLUMN_ID: the column we are updating and doing GC on
 * @oaram ITR: the iterator pointing to the latest (just updated) value entry <timestamp, Value> of
 * old_values_. We put this ITR into time_segment
 */
void RO6Row::garbageCollection(int column_id, std::map<i64, Value>::iterator itr) {
  // current system time in milliseconds
  i64 currentTime = static_cast<i64>(time(NULL) * 1000);
  i64 cutTime = currentTime - VERSION_SAFE_TIME;
  // put itr (along with current time) into time_segment
  time_segment[column_id].insert(std::pair<i64, std::map<i64, Value>::iterator>(currentTime, itr));
  for (auto it = time_segment[column_id].begin(); it != time_segment[column_id].end(); ++it) {
    if (it->first > cutTime) {
      // we remove all elements up to this entry for both old_values_ and time_segment
      if (it == time_segment[column_id].begin()) {
        break;
      }
      std::map<i64, Value>::iterator recorded_itr = (--it)->second;
      time_segment[column_id].erase(time_segment[column_id].begin(), ++it);
      old_values_[column_id].erase(old_values_[column_id].begin(), ++recorded_itr);
      break;
    }
  }
}

/*
 * RO-6: get most recent version number for this column.
 * Used when a write needs to record version number for a later read to query at
 */
version_t RO6Row::getCurrentVersion(int column_id) {
  auto &values = old_values_[column_id];

  if (values.size() == 0) {
    // current write piece is writing the first value to this column;
    // then the old version we return should be null, version number should
    // be 0
    return 0;
  }
  std::map<i64, Value>::iterator itr = values.end();
  return (--itr)->first;
}

/*
 * RO-6: get a value associated with a specific version number
 * @param COLUMN_ID: specify a column
 * @param VERSION_NUM: specify a version number (timestamp) to query with
 *
 * @return: an old value
 */
Value RO6Row::get_column_by_version(int column_id, i64 version_num) {
//    verify (old_values_.find(column_id) != old_values_.end());

  auto &values = old_values_[column_id];
  auto it = values.find(version_num);
  if (it == values.end()) {
    // I'm uneasy if I ever get to this case.....
    // Shuai: This is where the bug happens!
//        return Value("");
    return Row::get_column(column_id);
  }
  return it->second;
}

/*
 * RO-6: check ReadTxnIdTracker to get specific version number for reads to query with;
 * We also update it when we have writes coming with a vector of rtxn_ids (those ids were got via
 * that write's dep_check and will be put into Tracker for later possible reads)
 *
 * Write will update the stored version number for each rtxn_id
 * Read will simply fetch needed information
 */
version_t ReadTxnIdTracker::checkIfTxnIdBeenRecorded(int column_id,
                                                     i64 txnId,
                                                     bool forWrites,
                                                     version_t chosenVersion) {
  version_t txnTimeToReturn = 0;
  //recordTime is real time for garbage collection
  i64 recordTime = static_cast<i64>(time(NULL) * 1000);
  version_t txnTime = !forWrites ? 0 : chosenVersion;  // a place holder, txnTime should be filled in by writes
  // after done dep_check
  //prepare time entry for this transaction. A time entry has transaction's effective time and record time
  TxnTimes timesEntry = std::make_pair(txnTime, recordTime);
  //ReadTxnEntry txnIdList = keyToReadTxnIds.get(locatorKey);
  ReadTxnEntry *rtxn_entry = &keyToReadTxnIds[column_id];
  if (rtxn_entry->size() == 0) {
    //the locator_key is even not touched by other read txns yet
    ReadTxnEntry read_txn_entry;
    read_txn_entry[txnId] = timesEntry;
    *rtxn_entry = read_txn_entry;
    keyToLastAccessedTime[column_id] = recordTime;
  }
  else {
    i64 safetyTime = recordTime - VERSION_SAFE_TIME;
    // this key has not been checked by dep_check for a while, we need to explicitly do garbage collection
    if (keyToLastAccessedTime[column_id] < safetyTime) {
      for (std::pair<i64, TxnTimes> txn_entry : *rtxn_entry) {
        if (txn_entry.second.second < safetyTime) {
          rtxn_entry->erase(txn_entry.first);
        }
      }
      keyToLastAccessedTime[column_id] = recordTime;
    }
    //ReadTxnEntry read_txn_entry = keyToReadTxnIds[column_id];

    //ArrayList<Long> findTxnId = txnIdList.get(txnId);
    if (rtxn_entry->find(txnId) == rtxn_entry->end()) {
      // locator_key exists but this txnId is not in the record
      (*rtxn_entry)[txnId] = timesEntry;
    } else {
      // if we did find this txnId recorded before, then we return its effective time
      // if forWrites, to see if we need to update the txnTime, we keep the min of all txnTimes of this txnId
      txnTimeToReturn = (*rtxn_entry)[txnId].first;
      if (forWrites) {
        if (txnTimeToReturn > txnTime) {
          (*rtxn_entry)[txnId].first = txnTime; //update txnTime
        }
      }
    }
  }
  // if txnTimeToReturn not equal to 0, then it also means we found this txnId in our record
  return txnTimeToReturn;
}

/*
 * This will be called by a dep_checkee, the server that a write check dependencies against.
 * It will check Tracker to see if there were read transactions recorded. If so, get those Ids and
 * return them to coordinator, and then coordinator will pass them to the dep_checker server.
 */
std::vector<i64> ReadTxnIdTracker::getReadTxnIds(int column_id) {
  std::vector<i64> returnedIdList;
  if (keyToReadTxnIds.find(column_id) == keyToReadTxnIds.end())
    return returnedIdList;
  i64 currentTime = static_cast<i64>(time(NULL) * 1000);
  i64 safeTime = currentTime - VERSION_SAFE_TIME;
  ReadTxnEntry *rtxn_entry = &keyToReadTxnIds[column_id];
  for (std::pair<i64, TxnTimes> entry : *rtxn_entry) {
    if (entry.second.second >= safeTime) {
      returnedIdList.push_back(entry.first);
    } else {
      rtxn_entry->erase(entry.first);
    }
  }
  keyToLastAccessedTime[column_id] = currentTime;
  return returnedIdList;
}

/*
 * Comment this out here, but we need this functionality when move all implementation to upper level
 * -> TxSnow
 */
Value RO6Row::get_column(int column_id, i64 txnId) {
  version_t version_number = rtxn_tracker.checkIfTxnIdBeenRecorded(column_id, txnId, false, 0);
  if (version_number == 0) {
    // Actually it's possible to get into this case, when a read needs to return a version that before
    // the first value of that column is written. version number 0 is set by write txn. Let's return a
    // pre-set value for this case.
    Row::get_column(column_id);
    //return Value("Null");
  }
  return get_column_by_version(column_id, version_number);
}

} // namespace janus
