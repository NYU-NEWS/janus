
#include "marshal-value.h"
#include "command_marshaler.h"
#include "txn_chopper.h"

namespace rococo {

rrr::Marshal &operator<<(rrr::Marshal &m, const SimpleCommand &cmd) {
  m << cmd.id_;
  m << cmd.type_;
  m << cmd.inn_id_;
  m << cmd.root_id_;
  m << cmd.root_type_;
  m << cmd.input;
  m << cmd.output;
  m << cmd.output_size;
  m << cmd.par_id;
  return m;
}

rrr::Marshal &operator>>(rrr::Marshal &m, SimpleCommand &cmd) {
  m >> cmd.id_;
  m >> cmd.type_;
  m >> cmd.inn_id_;
  m >> cmd.root_id_;
  m >> cmd.root_type_;
  m >> cmd.input;
  m >> cmd.output;
  m >> cmd.output_size;
  m >> cmd.par_id;
  return m;
}

} // namespace rococo
