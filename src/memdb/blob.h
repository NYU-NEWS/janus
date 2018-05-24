#pragma once

#include "utils.h"

namespace mdb {

// reference to memory data, which is not managed by blob itself
struct blob {
    const char* data;
    int len;

    blob(): data(nullptr), len(0) { }

    bool operator == (const blob& other) const {
        return (len == other.len) && (memcmp(data, other.data, len) == 0);
    }

    class hash {
    public:
        size_t operator() (const blob& b) const {
            return stringhash32(b.data, b.len);
        }
    };
};

class MultiBlob {
    struct blob* blobs_;
    int count_;

public:
    explicit MultiBlob(int n = 0): blobs_(nullptr), count_(n) {
        if (count_ > 0) {
            blobs_ = new blob[count_];
        }
    }

    MultiBlob(const blob& b): count_(1) {
        blobs_ = new blob[count_];
        blobs_[0] = b;
    }

    MultiBlob(const MultiBlob& mb): blobs_(nullptr), count_(mb.count_) {
        if (count_ > 0) {
            blobs_ = new blob[count_];
        }
        for (int i = 0; i < count_; i++) {
            blobs_[i] = mb.blobs_[i];
        }
    }

    ~MultiBlob() {
        if (blobs_ != nullptr) {
            delete[] blobs_;
        }
    }

    int count() const {
        return count_;
    }

    const MultiBlob& operator= (const MultiBlob& o) {
        if (this != &o) {
            if (blobs_ != nullptr) {
                delete[] blobs_;
            }
            count_ = o.count_;
            if (count_ > 0) {
                blobs_ = new blob[count_];
            }
            for (int i = 0; i < count_; i++) {
                blobs_[i] = o.blobs_[i];
            }
        }
        return *this;
    }

    blob& operator[] (int idx) const {
        return blobs_[idx];
    }

    bool operator == (const MultiBlob& other) const {
        if (count_ == other.count_) {
            for (int i = 0; i < count_; i++) {
                if (!(blobs_[i] == other.blobs_[i])) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    class hash {
    public:
        size_t operator() (const MultiBlob& mb) const {
            size_t v = 0;
            blob::hash h;
            for (int i = 0; i < mb.count_; i++) {
                v ^= h(mb.blobs_[i]);
            }
            return v;
        }
    };
};

} // namespace mdb

