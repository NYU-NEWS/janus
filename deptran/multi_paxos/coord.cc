

#include "coord.h"

namespace rococo {

void MultiPaxosCoord::Submit(SimpleCommand& cmd, std::function<void()> func) {
  cmd_ = new SimpleCommand();
  *cmd_ = cmd;

}

} // namespace rococo