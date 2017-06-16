
#include "coroutine.h"
#include "event.h"
#include "scheduler.h"

namespace rrr {

void Event::Wait() {
  // TODO
  if (IsReady()) {
    return;
  } else {
    auto sp_coro = wp_coro_.lock();
    verify(sp_coro);
    sp_coro->Yield();
  }
  verify(0);
}


bool IntEvent::TestTrigger() {
  verify(status_ <= WAIT);
  if (value_ == target_) {
    status_ = READY;
    CoroScheduler::CurrentScheduler()->AddReadyEvent(*this);
    return true;
  }
  return false;
}


} // namespace rrr