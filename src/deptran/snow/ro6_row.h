#pragma once
#include "../__dep__.h"

namespace janus {

typedef uint64_t version_t;

// RO-6: do GC for every GC_THRESHOLD old values
const int GC_THRESHOLD = 10000;
// RO-6: GC time for striping out old versions = 5000 ms
const int VERSION_SAFE_TIME = 5000;

/*
 * RO-6: A class that keeps tracking of observed read txns.
 * A write (row_update/insert/delete) should update this class of the corresponding row first by
 * doing dep_check on a remote row, get rtxn_ids from there, and update its own row's keyToReadTxnIds map
 * A read will also check this map first to see if it needs to query an old version. The targetted version
 * number will be also included in this map if so.
 *
 * Should be instantiated only by RO6Row class privately.
 */
class ReadTxnIdTracker {
 public:
  std::map<int, i64> keyToLastAccessedTime;

  version_t checkIfTxnIdBeenRecorded(int column_id, i64 txnId, bool forWrites, version_t chosenVersion);

  std::vector<i64> getReadTxnIds(int column_id);

  void clearContext() {
    keyToReadTxnIds.clear();
    keyToLastAccessedTime.clear();
  }

 private:
  // a pair of information for each read txn (txn_id) we recorded.
  // first i64 is version number that will be used for later read. Second i64 is the real sys time when this txn_id
  // is recorded (for GC use)
  typedef std::pair<i64, i64> TxnTimes;
  // One entry is for one read txn. i64 is txn_id.
  typedef std::map<i64, TxnTimes> ReadTxnEntry;
  // The main data structure used to keep a list of recorded read txns.
  // "int" is column_id. We
  std::map<int, ReadTxnEntry> keyToReadTxnIds;
};

/*
 * RO-6: This class defines a row which keeps old versions of each column.
 * Old versions are stored in a map of maps <column_id, map<timestamp, value> >.
 * column_id is used to point to specific columns; then for each column, it stores
 * old values and each old value is associated a timestamp to uniquely identify each version.
 *
 * We use a global counter ver_s as timestamp.
 *
 * Then, O(1) for querying a specific old version; O(k/GC_THRESHOLD) for garbage collection - k is constant as number
 * of versions on average are kept within 5 secs.
 */

#define MultiVersionedRow RO6Row

class RO6Row: public RccRow {
 public:

  virtual mdb::symbol_t rtti() const {
    return mdb::symbol_t::ROW_MULTIVER;
  }

  void copy_into(RO6Row *row) const;


  virtual mdb::Row *copy() const {
    RO6Row *row = new RO6Row();
    copy_into(row);
    return row;
  }

  /*
   * Update the list of old versions for each row update
   * Overrides Row::update(~)
   * For update, simply push to the back of the map;
   *
   * We create similar functions for different augment types
   *
   * Note: We need to pass set of read_txn_ids taken on by each write txn all the way down to here
   */
  void update(int column_id, i64 v, const std::vector<i64> &txnIds) {
    update_internal(column_id, v, txnIds);
  }

  void update(int column_id, i32 v, const std::vector<i64> &txnIds) {
    update_internal(column_id, v, txnIds);
  }

  void update(int column_id, double v, const std::vector<i64> &txnIds) {
    update_internal(column_id, v, txnIds);
  }

  void update(int column_id, const std::string &str, const std::vector<i64> &txnIds) {
    update_internal(column_id, str, txnIds);
  }

  void update(int column_id, const Value &v, const std::vector<i64> &txnIds) {
    update_internal(column_id, v, txnIds);
  }

  /*
   * For update by column name
   */
  void update(const std::string &col_name, i64 v, const std::vector<i64> &txnIds) {
    this->update(schema_->get_column_id(col_name), v, txnIds);
  }

  void update(const std::string &col_name, i32 v, const std::vector<i64> &txnIds) {
    this->update(schema_->get_column_id(col_name), v, txnIds);
  }

  void update(const std::string &col_name, double v, const std::vector<i64> &txnIds) {
    this->update(schema_->get_column_id(col_name), v, txnIds);
  }

  void update(const std::string &col_name, const std::string &str, const std::vector<i64> &txnIds) {
    this->update(schema_->get_column_id(col_name), str, txnIds);
  }

  void update(const std::string &col_name, const Value &v, const std::vector<i64> &txnIds) {
    this->update(schema_->get_column_id(col_name), v, txnIds);
  }


  /*
   * For reads, we need the id for this read txn
   */
  Value get_column(int column_id, i64 txnId);

  Value get_column(const std::string &col_name, i64 txnId) {
    return get_column(schema_->get_column_id(col_name), txnId);
  }

  // retrieve current version number
  version_t getCurrentVersion(int column_id);

  // get a value specified by a version number
  Value get_column_by_version(int column_id, i64 version_num);

  // TODO: do some tests to see how slow it is

 private:
  static version_t ver_s;

  // garbage collection
  void garbageCollection(int column_id, std::map<i64, Value>::iterator itr);

  // Internal update logic, a template function to accomodate all types
  template<typename Type>
  void update_internal(int column_id, Type v, const std::vector<i64> &txnIds) {
    // first get current value before update, and put current value in old_values_
    Value currentValue = Row::get_column(column_id);
    // get current version
    version_t currentVersionNumber = getCurrentVersion(column_id);
    // push this new value to the old versions map
    std::pair<i64, Value> valueEntry = std::make_pair(next_version(), currentValue);
    // insert this old version to old_values_
    std::map<i64, Value>::iterator newElementItr = (old_values_[column_id].insert(valueEntry)).first;
    if (old_values_[column_id].size() % GC_THRESHOLD == 0) {
      // do Garbage Collection
      garbageCollection(column_id, newElementItr);
    }
    //Now we need to update rtxn_tracker first
    for (i64 txnId : txnIds) {
      rtxn_tracker.checkIfTxnIdBeenRecorded(column_id, txnId, true, currentVersionNumber);
    }
    // then update column as normal
    Row::update(column_id, v);
  }

 public:
  static version_t next_version() {
    return ++RO6Row::ver_s;
  }

  // one ReadTxnIdTracker object for each row, for tracking seen read trasactions
  // should be tracked and updated by *this* row's update and read.
  ReadTxnIdTracker rtxn_tracker;

  template<class Container>
  static RO6Row *create(const mdb::Schema *schema, const Container &values) {
    verify(values.size() == schema->columns_count());
    std::vector<const Value *> values_ptr(values.size(), nullptr);
    size_t fill_counter = 0;
    for (auto it = values.begin(); it != values.end(); ++it) {
      fill_values_ptr(schema, values_ptr, *it, fill_counter);
      fill_counter++;
    }
    RO6Row *raw_row = new RO6Row();
    raw_row->init_dep(schema->columns_count());
    return (RO6Row *) Row::create(raw_row, schema, values_ptr);
    // TODO (for haonan) initialize the data structure for a new row here.
  }
  /*
   * These functions are for testing purpoes. Uncomment them to use
   *
  int getTotalVerionNums(int column_id) {
      return old_values_[column_id].size();
  }

  i64 getVersionHead(int column_id) {
      return old_values_[column_id].begin()->second.get_i64();
  }

  i64 getVersionTail(int column_id) {
      return (--old_values_[column_id].end())->second.get_i64();
  }
   *
   */
 private:
  // data structure to keep all old versions for a row
  std::map<mdb::colid_t, std::map<i64, Value> > old_values_;
  // data structure to keep real time for each 100 versions. used for GC
  std::map<mdb::colid_t, std::map<i64, std::map<i64, Value>::iterator> > time_segment;
};

} // namespace janus
