
#include "executor.h"

namespace rococo {

Executor::Executor(cmdid_t cmd_id, Scheduler *sched)
    : cmd_id_(cmd_id), sched_(sched) {

}

} // namespace rococo