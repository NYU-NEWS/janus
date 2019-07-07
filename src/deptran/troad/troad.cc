
#include "../__dep__.h"
#include "../command.h"
#include "../command_marshaler.h"
#include "../communicator.h"
#include "../rococo/rcc_row.h"
#include "troad.h"

namespace janus {

static Frame *troad_frame_s = Frame::RegFrame(MODE_TROAD,
                                              {"troad"},
                                              []() -> Frame * {
                                                return new TroadFrame();
                                              });

} // namespace janus
