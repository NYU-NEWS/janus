#pragma once

#include "__dep__.h"

namespace rococo {

class SimpleCommand;
class MultiPaxosClient {

  void Submit(SimpleCommand& cmd);
};

} // namespace rococo

