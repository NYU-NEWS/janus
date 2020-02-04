#include "linked_vector.h"

namespace mdb {
    template <class T>
    LinkedVector<T>::LinkedVector() : nodes(), free_head(-1), _front(-1), _back(-1), _queue_size(0) {
        // default constructor
    }

    template <class T>
    LinkedVector<T>::LinkedVector(int size) {
        // initialize with size
        if (size <= 0) {
            LinkedVector();
        } else {
            nodes(size);
            free_head = -1;
            _front = 0;
            _back = size - 1;
            _queue_size = size;
        }
    }

    template <class T>
    LinkedVector<T>::LinkedVector(const LinkedVector<T>& lv) {
        nodes(lv.nodes);
        free_head = lv.free_head;
        _front = lv._front;
        _back = lv._back;
        _queue_size = lv._queue_size;
    }

    template <class T>
    LinkedVector<T>& LinkedVector<T>::operator=(const LinkedVector<T>& lv) {
        if (this == &lv) {
            return *this;
        }
        nodes.clear();
        nodes = lv.nodes;
        free_head = lv.free_head;
        _front = lv._front;
        _back = lv._back;
        _queue_size = lv._queue_size;
        return *this;
    }

    template <class T>
    LinkedVector<T>::~LinkedVector() {
        // destruct each node
        for (auto& node : nodes) {
            ~node();
        }
    }

    template <class T>
    int LinkedVector<T>::front() {
        return _front;
    }

    template <class T>
    int LinkedVector<T>::back() {
        return _back;
    }

    template <class T>
    int LinkedVector<T>::vector_size() {
        return nodes.size();
    }

    template <class T>
    int LinkedVector<T>::queue_size() {
        return _queue_size;
    }

    template <class T>
    bool LinkedVector<T>::contains(int index) {
        return !nodes.at(index).free;
    }

    template <class T>
    bool LinkedVector<T>::push_back(Node<T> node) {
        if (free_head == -1) {
            // no prev removed nodes, no free positions
            nodes.push_back(std::move(node));   // use std::move, avoid copy
            nodes[_back].next = nodes.size() - 1;
            nodes.back().prev = _back;
            _back = nodes.size() - 1;
        } else {
            // there is free slot
            int free_slot = free_head;
            free_head = nodes[free_slot].next;
            nodes[free_slot] = std::move(node);
            nodes[_back].next = free_slot;
            nodes[free_slot].prev = _back;
            _back = free_slot;
        }
        ++_queue_size;
        return true;
    }

    template <class T>
    bool LinkedVector<T>::remove(int index) {
        if (nodes.size() <= index || nodes[index].free) {
            // out of range or position has been removed
            return false;
        }
        if (nodes[index].prev != -1) {
            // this is not the head
            nodes[nodes[index].prev].next = nodes[index].next;
        }
        if (nodes[index].next != -1) {
            // this is not the tail
            nodes[nodes[index].next].prev = nodes[index].prev;
        }
        nodes[index].next = free_head;
        nodes[index].prev = -1;
        nodes[index].free = true;
        free_head = index;
        --_queue_size;
        return true;
    }

    template <class T>
    bool LinkedVector<T>::clear() {
        nodes.clear();
        free_head = -1;
        _front = -1;
        _back = -1;
        _queue_size = 0;
    }
}
