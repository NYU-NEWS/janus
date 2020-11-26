#include "coordinator.h"

namespace janus {
    void CoordinatorAcc::GotoNextPhase() {
        int n_phase = 2;
        switch (phase_++ % n_phase) {
            case Phase::INIT_END:DispatchAsync();
                verify(phase_ % n_phase == Phase::DISPATCH);
                break;
            case Phase::DISPATCH:
                committed_ = true;
                verify(phase_ % n_phase == Phase::INIT_END);
                End();
                break;
            default:
                verify(0);
        }
    }
} // namespace janus