#pragma once

#include "elog/broker/produce_handler.h"
#include "elog/protocol/frame.h"

namespace elog {

// Routes one request frame to the handler for its API key and returns the response frame
// (LLD §6.1). A key with no handler gets an error-only INVALID_REQUEST response (R-10).
//
// Thread rules: dispatch() is called concurrently by every connection thread; it keeps no
// state of its own, and the handlers it calls are safe to call concurrently.
class RequestDispatcher {
public:
    explicit RequestDispatcher(ProduceHandler& produce);

    Frame dispatch(const Frame& request);

private:
    Frame handleProduce(const Frame& request);

    ProduceHandler& produce_;
};

}  // namespace elog
