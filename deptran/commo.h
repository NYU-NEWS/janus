
#pragma once

#include "all.h"

namespace rococo {

class Commo {

  template<typename T>
  static void broadcast(groupid_t, T, std::function<void(groupid_t, void*)>, std::function<void(groupid_t)>);
};
} // namespace
