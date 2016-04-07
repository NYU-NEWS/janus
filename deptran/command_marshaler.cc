#include "__dep__.h"
#include "marshal-value.h"
#include "command.h"
#include "command_marshaler.h"
#include "txn_chopper.h"

namespace rococo {

rrr::Marshal &operator<<(rrr::Marshal &m, const ContainerCommand &cmd) {
  m << cmd.id_;
  m << cmd.type_;
  m << cmd.inn_id_;
  m << cmd.root_id_;
  m << cmd.root_type_;
  verify(cmd.self_cmd_);
  cmd.self_cmd_->ToMarshal(m);
  return m;
}

rrr::Marshal &operator>>(rrr::Marshal &m, ContainerCommand &cmd) {
  m >> cmd.id_;
  m >> cmd.type_;
  m >> cmd.inn_id_;
  m >> cmd.root_id_;
  m >> cmd.root_type_;
  auto it = cmd.Initializers().find(cmd.type_);
  verify(it != cmd.Initializers().end());
  cmd.self_cmd_ = it->second();
  cmd.self_cmd_->FromMarshal(m);
  return m;
}


rrr::Marshal &operator<<(rrr::Marshal &m, const SimpleCommand &cmd) {
  if (cmd.input.size() == 0) verify(cmd.input.begin() == cmd.input.end());
  verify(cmd.input.size() < 10000);
  m << cmd.id_;
  m << cmd.type_;
  m << cmd.inn_id_;
  m << cmd.root_id_;
  m << cmd.root_type_;
  m << cmd.input;
  m << cmd.output;
  m << cmd.output_size;
  m << cmd.partition_id_;
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
  m >> cmd.partition_id_;
  return m;
}


int ContainerCommand::RegInitializer(cmdtype_t cmd_type,
                                     function<ContainerCommand*()> init) {
  auto pair = Initializers().insert(std::make_pair(cmd_type, init));
  verify(pair.second);
}

function<ContainerCommand*()>
ContainerCommand::GetInitializer(cmdtype_t type) {
  auto it = Initializers().find(type);
  verify(it != Initializers().end());
  return it->second;
}

map<cmdtype_t, function<ContainerCommand*()>>&
ContainerCommand::Initializers() {
  static map<cmdtype_t, function<ContainerCommand*()>> m;
  return m;
};

} // namespace rococo
