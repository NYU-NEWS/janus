
#include "coroutine.h"
#include "event.h"
#include "scheduler.h"

namespace rrr {

void Event::Wait() {
  // TODO
  if (IsReady()) {
    return;
  } else {
    // for now, only the coroutine where the event was created can wait on the event.
    auto sp_coro = wp_coro_.lock();
    verify(sp_coro);
    verify(sp_coro == Coroutine::CurrentCoroutine());
    sp_coro->Yield();
  }
  verify(0);
}


bool IntEvent::TestTrigger() {
  verify(status_ <= WAIT);
  if (value_ == target_) {
    if (status_ == INIT) {
      // do nothing until wait happens.
    } else if (status_ == WAIT) {
      CoroScheduler::CurrentScheduler()->AddReadyEvent(*this);
    } else {
      verify(0);
    }
    status_ = READY;
    return true;
  }
  return false;
}


} // namespace rrr