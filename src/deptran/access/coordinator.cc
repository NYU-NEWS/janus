#include "coordinator.h"

namespace janus {
    void CoordinatorAcc::GotoNextPhase() {
        const int n_phase = 4;
        switch (phase_++ % n_phase) {
            case Phase::INIT_END:
                DispatchAsync();
                verify(phase_ % n_phase == Phase::DISPATCH);
                break;
            case Phase::DISPATCH:
                // TODO: safeguard check. If consistent, respond.
                committed_ = true;
                verify(phase_ % n_phase == Phase::VALIDATE);
                End();
                break;
            case Phase::VALIDATE:
                verify(phase_ % n_phase == Phase::DECIDE);
                break;
            case Phase::DECIDE:
                verify(phase_ % n_phase == Phase::INIT_END);
                break;
            default:
                verify(0);  // invalid phase
                break;
        }
    }
} // namespace janus