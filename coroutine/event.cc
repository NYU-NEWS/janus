
#include "coroutine.h"
#include "event.h"
#include "scheduler.h"

namespace rrr {

void Event::Wait() {
  // TODO
  if (IsReady()) {
    return;
  } else {
    coro_->Yield();
  }
  verify(0);
}


bool IntEvent::TestTrigger() {
  verify(status_ <= WAIT);
  if (value_ == target_) {
    status_ = READY;
    sched_->AddReadyEvent(this);
    return true;
  }
  return false;
}


} // namespace rrr