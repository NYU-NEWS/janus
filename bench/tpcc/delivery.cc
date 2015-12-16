#include "all.h"

namespace rococo {

void TpccPiece::reg_delivery() {
  BEGIN_PIE(TPCC_DELIVERY,
          TPCC_DELIVERY_0, // Ri & W new_order
          DF_FAKE) {
      // this is a little bit tricky, the first half will do most of the job,
      // removing the row from the table, but it won't actually release the
      // resource. And the bottom half is in charge of release the resource,
      // including the vertex entry

      Log::debug("TPCC_DELIVERY, piece: %d", TPCC_DELIVERY_0);
      verify(input_size == 2);
      i32 oi = 0;
      Value buf;
//        mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                         mdb::Txn *txn = dtxn->mdb_txn_;
      //cell_locator_t cl(TPCC_TB_NEW_ORDER, 3);
      mdb::Row *r = NULL;
      mdb::Table *tbl = dtxn->GetTable(TPCC_TB_NEW_ORDER);
      if (!(IS_MODE_RCC || IS_MODE_RO6) ||
              ((IS_MODE_RCC || IS_MODE_RO6) && IN_PHASE_1)) {
          // non-rcc || rcc start request
          mdb::MultiBlob mbl(3), mbh(3);
          mbl[0] = input[1].get_blob();
          mbh[0] = input[1].get_blob();
          mbl[1] = input[0].get_blob();
          mbh[1] = input[0].get_blob();
          Value no_o_id_low(std::numeric_limits<i32>::min()),
                  no_o_id_high(std::numeric_limits<i32>::max());
          mbl[2] = no_o_id_low.get_blob();
          mbh[2] = no_o_id_high.get_blob();

          mdb::ResultSet rs = txn->query_in(tbl, mbl, mbh,
                  output_size, header.pid, mdb::ORD_ASC);

          if (rs.has_next()) {
              r = rs.next();

              if (IS_MODE_2PL && output_size == NULL) {
                  mdb::Txn2PL::PieceStatus *ps
                          = ((mdb::Txn2PL *) txn)->get_piece_status(header.pid);

                  std::function<void(void)> succ_callback =
                      ((TPLExecutor*) exec)->get_2pl_succ_callback(
                          header, input, input_size, res, ps);

                  std::function<void(void)> fail_callback =
                      ((TPLExecutor*) exec)->get_2pl_fail_callback(
                          header, res, ps);

                  ps->reg_rm_lock(r, succ_callback, fail_callback);

                  return;
              }
              txn->read_column(r, 2, &buf);
              output[oi++] = buf;
          } else {
              TPL_KISS_NONE;
              output[oi++] = Value((i32) -1);
          }
      }

      if (IS_MODE_RCC || IS_MODE_RO6) { // deptran
          if (IN_PHASE_1) { // deptran start req, top half
              if (r) { // if find a row
                  // FIXME!!!!!
                  RCC_KISS(r, 0, true);
                  RCC_KISS(r, 1, true);
                  RCC_KISS(r, 2, true);
                  static int iiiii = 0;
                  RCC_SAVE_ROW(r, iiiii++);
                  tbl->remove(r, false); // don't release the row
              }
          } else { // deptran finish
            auto &row_map = ((RCCDTxn*)dtxn)->dreqs_.back().row_map;
              for (auto &it : row_map) {
                  it.second->release();
              }
          }
      } else { // non deptran
          if (r) {
              txn->remove_row(tbl, r);
          }
      }

      // ##############################################
      verify(*output_size >= oi);
      *output_size = oi;
      Log::debug("TPCC_DELIVERY, piece: %d end", TPCC_DELIVERY_0);
      // ##############################################
      *res = SUCCESS;
      return;
  } END_PIE

  BEGIN_PIE(TPCC_DELIVERY,
          TPCC_DELIVERY_1, // Ri & W order
          DF_NO) {

      Log::debug("TPCC_DELIVERY, piece: %d", TPCC_DELIVERY_1);
      verify(input_size == 4);
      i32 oi = 0;
      Value buf;
//        mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                       mdb::Txn *txn = dtxn->mdb_txn_;
      mdb::MultiBlob mb(3);
      //cell_locator_t cl(TPCC_TB_ORDER, 3);
      mb[0] = input[2].get_blob();
      mb[1] = input[1].get_blob();
      mb[2] = input[0].get_blob();
      //Log::debug("Delivery: o_d_id: %d, o_w_id: %d, o_id: %d, hash: %u", input[2].get_i32(), input[1].get_i32(), input[0].get_i32(), mdb::MultiBlob::hash()(cl.primary_key));

      mdb::Row *r = txn->query(txn->get_table(TPCC_TB_ORDER), mb,
              output_size, header.pid).next();

      TPL_KISS(
              mdb::column_lock_t(r, 3, ALock::RLOCK),
              mdb::column_lock_t(r, 5, ALock::WLOCK)
      );

      if ((IS_MODE_RCC || IS_MODE_RO6) && IN_PHASE_1) {
          ((RCCDTxn *) dtxn)->kiss(r, 5, true);
      }

      txn->read_column(r, 3, &output[oi++]); // read o_c_id
      txn->write_column(r, 5, input[3]); // write o_carrier_id

      // ##############################################
      verify(*output_size >= oi);
      *output_size = oi;
      Log::debug("TPCC_DELIVERY, piece: %d end", TPCC_DELIVERY_1);
      // ##############################################
      *res = SUCCESS;
      return;
  } END_PIE


  BEGIN_PIE(TPCC_DELIVERY,
          TPCC_DELIVERY_2, // Ri & W order_line
          DF_NO) {

      Log::debug("TPCC_DELIVERY, piece: %d", TPCC_DELIVERY_2);
      verify(input_size == 3);
      i32 oi = 0;
      Value buf;
//        mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                       mdb::Txn *txn = dtxn->mdb_txn_;
      mdb::MultiBlob mbl(4), mbh(4);
      mbl[0] = input[2].get_blob();
      mbh[0] = input[2].get_blob();
      mbl[1] = input[1].get_blob();
      mbh[1] = input[1].get_blob();
      mbl[2] = input[0].get_blob();
      mbh[2] = input[0].get_blob();
      Value ol_number_low(std::numeric_limits<i32>::min()),
              ol_number_high(std::numeric_limits<i32>::max());
      mbl[3] = ol_number_low.get_blob();
      mbh[3] = ol_number_high.get_blob();

      mdb::ResultSet rs = txn->query_in(
              txn->get_table(TPCC_TB_ORDER_LINE), mbl, mbh,
              output_size, header.pid, mdb::ORD_ASC);
      double ol_amount_buf = 0.0;
      mdb::Row *r = NULL;
//                cell_locator_t cl(TPCC_TB_ORDER_LINE, 4);
//                cl.primary_key[0] = input[2].get_blob();
//                cl.primary_key[1] = input[1].get_blob();
//                cl.primary_key[2] = input[0].get_blob();

      std::vector<mdb::Row *> row_list;
      row_list.reserve(15);
      while (rs.has_next()) {
          row_list.push_back(rs.next());
      }

      if (IS_MODE_2PL && output_size == NULL) {
          mdb::Txn2PL::PieceStatus *ps
                  = ((mdb::Txn2PL *) txn)->get_piece_status(header.pid);

          std::function<void(void)> succ_callback =
              ((TPLExecutor*) exec)->get_2pl_succ_callback(
                  header, input, input_size, res, ps);

          std::function<void(void)> fail_callback =
              ((TPLExecutor*) exec)->get_2pl_fail_callback(
                  header, res, ps);

          std::vector<mdb::column_lock_t> column_locks;
          column_locks.reserve(2 * row_list.size());

          int i = 0;
          while (i < row_list.size()) {
              r = row_list[i++];

              column_locks.push_back(
                      mdb::column_lock_t(r, 6, ALock::WLOCK)
              );
              column_locks.push_back(
                      mdb::column_lock_t(r, 8, ALock::RLOCK)
              );
          }
          ps->reg_rw_lock(column_locks, succ_callback, fail_callback);

          return;
      }

      int i = 0;
      while (i < row_list.size()) {
          r = row_list[i++];

          if ((IS_MODE_RCC || IS_MODE_RO6) && IN_PHASE_1) {
              //cl.primary_key[3] = r->get_blob(3);
              //cl.col_id = 6;
              ((RCCDTxn *) dtxn)->kiss(r, 6, true);
              ((RCCDTxn *) dtxn)->kiss(r, 8, true);
          }

          txn->read_column(r, 8, &buf); // read ol_amount
          ol_amount_buf += buf.get_double();
          txn->write_column(r, 6, Value(std::to_string(time(NULL))));
      }
      output[oi++] = Value(ol_amount_buf);

      // ##############################################
      verify(*output_size >= oi);
      *output_size = oi;
      *res = SUCCESS;
      Log::debug("TPCC_DELIVERY, piece: %d end", TPCC_DELIVERY_2);
      // ##############################################
  } END_PIE

  BEGIN_PIE(TPCC_DELIVERY,
          TPCC_DELIVERY_3, // W customer
          DF_REAL) {
      Log::debug("TPCC_DELIVERY, piece: %d", TPCC_DELIVERY_3);
      verify(input_size == 4);
      i32 oi = 0;
      Value buf;
//        mdb::Txn *txn = DTxnMgr::get_sole_mgr()->get_mdb_txn(header);
                         mdb::Txn *txn = dtxn->mdb_txn_;
      mdb::Row *r = NULL;
      mdb::MultiBlob mb(3);
      //cell_locator_t cl(TPCC_TB_CUSTOMER, 3);
      mb[0] = input[0].get_blob();
      mb[1] = input[2].get_blob();
      mb[2] = input[1].get_blob();

      if (!(IS_MODE_RCC || IS_MODE_RO6) ||
              ((IS_MODE_RCC || IS_MODE_RO6) && IN_PHASE_1)) {
          // non-rcc || rcc start request
          r = txn->query(txn->get_table(TPCC_TB_CUSTOMER), mb,
                  output_size, header.pid).next();
      }

      TPL_KISS(
              mdb::column_lock_t(r, 16, ALock::WLOCK),
              mdb::column_lock_t(r, 19, ALock::WLOCK)
      );

      RCC_KISS(r, 16, false);
      RCC_KISS(r, 19, false);
      RCC_SAVE_ROW(r, TPCC_DELIVERY_3);
      RCC_PHASE1_RET;
      RCC_LOAD_ROW(r, TPCC_DELIVERY_3);

      txn->read_column(r, 16, &buf);
      buf.set_double(buf.get_double() + input[3].get_double());

      txn->write_column(r, 16, buf);
      txn->read_column(r, 19, &buf);
      buf.set_i32(buf.get_i32() + (i32) 1);
      txn->write_column(r, 19, buf);

      // ##############################################
      verify(*output_size >= oi);
      *res = SUCCESS;
      Log::debug("TPCC_DELIVERY, piece: %d end", TPCC_DELIVERY_3);
      // ##############################################
      *output_size = oi;
  } END_PIE
}

} // namespace rococo
