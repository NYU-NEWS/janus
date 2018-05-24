#pragma once

#include <unordered_set>

#include "utils.h"

namespace mdb {

typedef i64 lock_owner_t;

class RWLock {
    bool wlocked_;
    lock_owner_t w_;    // write access owner
    std::unordered_set<lock_owner_t> r_; // read access owner

public:

    RWLock(): wlocked_(false), w_(0) {}
    bool is_wlocked() const {
        assert(!wlocked_ || r_.empty());
        return wlocked_;
    }
    bool is_rlocked() const {
        assert(!wlocked_ || r_.empty());
        return !r_.empty();
    }
    bool wlock_by(lock_owner_t o) {
        if (wlocked_) {
            return o == w_;
        } else if (r_.empty()) {
            wlocked_ = true;
            w_ = o;
            return true;
        } else {
            // already rlocked, check if can upgrade lock
            auto it = r_.find(o);
            if (it != r_.end() && r_.size() == 1) {
                // lock upgrade
                r_.erase(it);
                wlocked_ = true;
                w_ = o;
                return true;
            } else {
                return false;
            }
        }
    }
    bool rlock_by(lock_owner_t o) {
        if (wlocked_) {
            return o == w_;
        } else {
            r_.insert(o);
            return true;
        }
    }
    bool unlock_by(lock_owner_t o) {
        bool ret = false;
        if (wlocked_ && o == w_) {
            wlocked_ = false;
            assert(r_.find(o) == r_.end());
            ret = true;
        }
        if (r_.erase(o) != 0) {
            ret = true;
        }
        return ret;
    }
    lock_owner_t wlock_owner() const {
        verify(wlocked_);
        return w_;
    }
    const std::unordered_set<lock_owner_t>& rlock_owner() const {
        return r_;
    }
};

}
