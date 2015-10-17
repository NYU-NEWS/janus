#pragma once 

#include "__dep__.h"
#include "constants.h"
#include "command.h"

namespace rococo {

struct StartRequest {
  cmdid_t cmd_id;
  cmdid_t pie_id; // TODO obsolete
  Command *cmd; 
};

struct StartReply {
  int res;
  Command *cmd; 
};

} // namespace rococo
