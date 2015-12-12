#pragma once
#include "txn.h"

namespace mdb {

class Txn2PL: public Txn {
 private:
  void release_piece_map(bool commit);
  void release_resource();

 protected:
  symbol_t outcome_;
  std::multimap<Row *, column_id_t> reads_;
  std::multimap<Row *, std::pair<column_id_t, Value>> updates_;
  std::multiset<table_row_pair> inserts_;
  std::unordered_set<table_row_pair, table_row_pair::hash> removes_;

  bool debug_check_row_valid(Row *row) const {
    for (auto &it : removes_) {
      if (it.row == row) {
        return false;
      }
    }
    return true;
  }

  ResultSet do_query(Table *tbl, const MultiBlob &mb);

  ResultSet do_query_lt(Table *tbl,
                        const SortedMultiKey &smk,
                        symbol_t order = symbol_t::ORD_ASC);
  ResultSet do_query_gt(Table *tbl,
                        const SortedMultiKey &smk,
                        symbol_t order = symbol_t::ORD_ASC);
  ResultSet do_query_in(Table *tbl,
                        const SortedMultiKey &low,
                        const SortedMultiKey &high,
                        symbol_t order = symbol_t::ORD_ASC);

  ResultSet do_all(Table *tbl, symbol_t order = symbol_t::ORD_ANY);

 public:
  class PieceStatus {
    friend class Txn2PL;
    //FIXME when locking thread starts to process a piece, and main thread starts to abort one txn
   private:
    static int64_t swap_bits(int64_t num) {
      int64_t ret = num << 32;
      ret |= (num >> 32) & 0x00000000FFFFFFFF;
      return ret;
    }
    i64 pid_;
    int num_waiting_;
    int num_acquired_;
    bool rej_;
    rrr::DragonBall *reply_db_;
    mdb::Value *output_buf_;
    rrr::i32 *output_size_buf_;
    std::vector<mdb::Value> *output_vec_;
    bool finish_;

    bool rw_succ_;
    rrr::ALockGroup rw_lock_group_;
    bool rm_succ_;
    rrr::ALockGroup rm_lock_group_;
    bool is_rw_;

    bool *wound_;

    query_buf_t query_buf_;
    Txn2PL *txn_;

    PieceStatus() : rw_lock_group_(0), rm_lock_group_(0) {
      verify(0);
    }

    PieceStatus(const PieceStatus &ps) : rw_lock_group_(0),
                                         rm_lock_group_(0) {
      verify(0);
    }

   public:
    PieceStatus(i64 tid, i64 pid, rrr::DragonBall *db, mdb::Value *output,
                rrr::i32 *output_size, bool *wound,
                const std::function<int(void)> &wound_callback,
                Txn2PL* txn) :
        pid_(pid),
        num_waiting_(1),
        num_acquired_(0),
        rej_(false),
        reply_db_(db),
        output_buf_(output),
        output_size_buf_(output_size),
        output_vec_(NULL),
        finish_(false),
        rw_succ_(false),
        rw_lock_group_(swap_bits(tid), wound_callback),
        rm_succ_(false),
        rm_lock_group_(swap_bits(tid), wound_callback),
        is_rw_(false),
        wound_(wound),
        query_buf_(query_buf_t()),
        txn_(txn) {
    }

    PieceStatus(i64 tid, i64 pid, rrr::DragonBall *db,
                std::vector<mdb::Value> *output, bool *wound,
                const std::function<int(void)> &wound_callback,
                Txn2PL* txn) :
        pid_(pid),
        num_waiting_(1),
        num_acquired_(0),
        rej_(false),
        reply_db_(db),
        output_buf_(NULL),
        output_size_buf_(NULL),
        output_vec_(output),
        finish_(false),
        rw_succ_(false),
        rw_lock_group_(swap_bits(tid), wound_callback),
        rm_succ_(false),
        rm_lock_group_(swap_bits(tid), wound_callback),
        is_rw_(false),
        wound_(wound),
        query_buf_(query_buf_t()),
        txn_(txn) {
    }

    ~PieceStatus() {
    }

    bool is_rejected() {
      return rej_ || *wound_;
    }

    void reg_rm_lock(Row *row,
                     const std::function<void(void)> &succ_callback,
                     const std::function<void(void)> &fail_callback);

    void reg_rw_lock(const std::vector<column_lock_t> &col_locks,
                     const std::function<void(void)> &succ_callback,
                     const std::function<void(void)> &fail_callback);

    void abort() {
      verify(finish_);
      if (rw_succ_)
        rw_lock_group_.unlock_all();
      if (rm_succ_)
        rm_lock_group_.unlock_all();
    }

    void commit() {
      verify(finish_);
      if (rw_succ_)
        rw_lock_group_.unlock_all();
    }

    void remove_output() {
      if (output_buf_) {
        verify(output_size_buf_ != NULL);
        *output_size_buf_ = 0;
      }
      else if (output_vec_) {
        output_vec_->resize(0);
      }
    }

    void set_num_waiting_locks(int num) {
      //TODO (right now, only supporting only either rw or delete, not both)
      verify(num <= 1);
      num_waiting_ = num;
      num_acquired_ = 0;
    }

    void start_yes_callback() {
      txn_->SetPsCache(this);
      num_acquired_++;
      verify(num_acquired_ <= num_waiting_);
      if (is_rw_)
        rw_succ_ = true;
      else
        rm_succ_ = true;
    }

    void start_no_callback() {
      txn_->SetPsCache(this);
      num_acquired_++;
      rej_ = true;
      verify(num_acquired_ <= num_waiting_);
    }

    void get_output(std::vector<mdb::Value> **output_vec,
                    mdb::Value **output, rrr::i32 **output_size) {
      *output_vec = output_vec_;
      *output = output_buf_;
      *output_size = output_size_buf_;
    }

    bool can_proceed() {
      return num_waiting_ == num_acquired_;
    }

    void trigger_reply_dragonball() {
      reply_db_->trigger();
    }

    void set_finish() {
      verify(!finish_);
      finish_ = true;
    }

  };

 private:
  bool wound_, prepared_;

  std::unordered_map<i64, PieceStatus *> piece_map_;
  //  static PieceStatus *ps_cache_s; // TODO fix
  PieceStatus *ps_cache_;

 public:

  Txn2PL() = delete;
  Txn2PL(const TxnMgr *mgr, txn_id_t txnid);
  ~Txn2PL();

  PieceStatus* ps_cache();
  void SetPsCache(PieceStatus*);
  query_buf_t& GetQueryBuf(int64_t);
  virtual bool commit_prepare() {
    prepared_ = true;
    if (wound_)
      return false;
    else
      return true;
  }

  bool is_wound() {
    return wound_;
  }

  virtual symbol_t rtti() const {
    return symbol_t::TXN_2PL;
  }

  virtual void marshal_stage(std::string &str);

  void abort();
  bool commit();

  void init_piece(i64 tid, i64 pid, rrr::DragonBall *db, mdb::Value *output,
                  rrr::i32 *output_size);
  void init_piece(i64 tid, i64 pid, rrr::DragonBall *db,
                  std::vector<mdb::Value> *output);

  PieceStatus *get_piece_status(i64 pid);

//    void reg_rm_lock_group(Row *row,
//            const std::function<void(void)> &succ_callback,
//            const std::function<void(void)> &fail_callback);
//
//    void reg_lock_group(const std::vector<column_lock_t> &col_locks,
//            const std::function<void(void)> &succ_callback,
//            const std::function<void(void)> &fail_callback);
//
  void reg_read_column(Row *row,
                       column_id_t col_id,
                       std::function<void(void)> succ_callback,
                       std::function<void(void)> fail_callback);
  void reg_write_column(Row *row,
                        column_id_t col_id,
                        std::function<void(void)> succ_callback,
                        std::function<void(void)> fail_callback);
  virtual bool read_column(Row *row, column_id_t col_id, Value *value);
  virtual bool write_column(Row *row, column_id_t col_id, const Value &value);
  virtual bool insert_row(Table *tbl, Row *row);
  virtual bool remove_row(Table *tbl, Row *row);

  virtual ResultSet query(Table *tbl, const MultiBlob &mb) {
    return do_query(tbl, mb);
  }
  virtual ResultSet query(Table *tbl,
                          const MultiBlob &mb,
                          bool retrieve,
                          int64_t pid);
  virtual ResultSet query_lt(Table *tbl,
                             const SortedMultiKey &smk,
                             symbol_t order = symbol_t::ORD_ASC) {
    return do_query_lt(tbl, smk, order);
  }
  virtual ResultSet query_lt(Table *tbl,
                             const SortedMultiKey &smk,
                             bool retrieve,
                             int64_t pid,
                             symbol_t order = symbol_t::ORD_ASC) {
    query_buf_t &qb = GetQueryBuf(pid);
    if (retrieve) {
      ResultSet rs = qb.buf[qb.retrieve_index++];
      rs.reset();
      return rs;
    }
    else {
      ResultSet rs = do_query_lt(tbl, smk, order);
      qb.buf.push_back(rs);
      return rs;
    }
  }
  virtual ResultSet query_gt(Table *tbl,
                             const SortedMultiKey &smk,
                             symbol_t order = symbol_t::ORD_ASC) {
    return do_query_gt(tbl, smk, order);
  }
  virtual ResultSet query_gt(Table *tbl,
                             const SortedMultiKey &smk,
                             bool retrieve,
                             int64_t pid,
                             symbol_t order = symbol_t::ORD_ASC) {
    query_buf_t &qb = GetQueryBuf(pid);
    if (retrieve) {
      ResultSet rs = qb.buf[qb.retrieve_index++];
      rs.reset();
      return rs;
    }
    else {
      ResultSet rs = do_query_gt(tbl, smk, order);
      qb.buf.push_back(rs);
      return rs;
    }
  }
  virtual ResultSet query_in(Table *tbl,
                             const SortedMultiKey &low,
                             const SortedMultiKey &high,
                             symbol_t order = symbol_t::ORD_ASC) {
    return do_query_in(tbl, low, high, order);
  }
  virtual ResultSet query_in(Table *tbl,
                             const SortedMultiKey &low,
                             const SortedMultiKey &high,
                             bool retrieve,
                             int64_t pid,
                             symbol_t order = symbol_t::ORD_ASC) {
    query_buf_t &qb = GetQueryBuf(pid);
    if (retrieve) {
      ResultSet rs = qb.buf[qb.retrieve_index++];
      rs.reset();
      return rs;
    }
    else {
      ResultSet rs = do_query_in(tbl, low, high, order);
      qb.buf.push_back(rs);
      return rs;
    }
  }
  virtual ResultSet all(Table *tbl, symbol_t order = symbol_t::ORD_ANY) {
    return do_all(tbl, order);
  }
  virtual ResultSet all(Table *tbl,
                        bool retrieve,
                        int64_t pid,
                        symbol_t order = symbol_t::ORD_ANY) {
    query_buf_t &qb = GetQueryBuf(pid);
    if (retrieve) {
      ResultSet rs = qb.buf[qb.retrieve_index++];
      rs.reset();
      return rs;
    }
    else {
      ResultSet rs = do_all(tbl, order);
      qb.buf.push_back(rs);
      return rs;
    }
  }
};




class TxnMgr2PL: public TxnMgr {
  std::multimap<Row *, std::pair<column_id_t, version_t>> vers_;
 public:
  virtual Txn *start(txn_id_t txnid) {
    return new Txn2PL(this, txnid);
  }
  virtual symbol_t rtti() const {
    return symbol_t::TXN_2PL;
  }
};


struct row_column_pair {
  Row *row;
  column_id_t col_id;

  row_column_pair(Row *r, column_id_t c) : row(r), col_id(c) { }

  bool operator==(const row_column_pair &o) const {
    return row == o.row && col_id == o.col_id;
  }

  struct hash {
    size_t operator()(const row_column_pair &p) const {
      size_t v1 = size_t(p.row);
      size_t v2 = size_t(p.col_id);
      return inthash64(v1, v2);
    }
  };
};


} // namespace mdb