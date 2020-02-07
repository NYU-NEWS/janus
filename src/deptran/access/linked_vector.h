//
// Created by chance_Lv on 2020/2/3.
//
#pragma once

#include <type_traits>
#include <vector>

using std::aligned_storage_t;
/*
 * Customized data structure for managing transaction queue
 * on each data item (key+col pair).
 * A TxnQueue is a linked vector
 *      -- insertion/removal: O(1)
 *      -- retrieval: O(1)
 *      -- in-place
 * An element in TxnQueue is a txn (a struct)
 * It's made general purpose
 */
namespace janus {
    template <class T>
    struct Node {
        // the element in TxnQueue, it will be a txn struct
        typename std::aligned_storage<sizeof(T), alignof(T)>::type element;
        int _next;   // point to next queued txn
        int _prev;   // point to previous txn
        int _pos;    // index of this node
        bool free;   // if this node position is free

        Node() : element(), _next(-1), _prev(-1), _pos(-1), free(false) {}  // a dangling node points to -1
        explicit Node(T&& x) : _next(-1), _prev(-1), _pos(-1), free(false) {
            new(&element) T(std::move(x));
        }
        Node(Node<T>&&) noexcept = default;
        Node<T>& operator=(Node<T>&& other) noexcept {
            element = std::move(other.element);
            _next = std::move(other._next);
            _prev = std::move(other._prev);
            _pos = std::move(other._pos);
            free = std::move(other.free);
            return *this;
        }
        explicit Node<T>(const Node<T>&) = delete;
        Node<T>& operator=(const Node<T>&) = delete;

        T& get_element() {
            return *reinterpret_cast<T*>(&element);
        }
        int get_pos() {
            return _pos;
        }
        ~Node() {
            reinterpret_cast<T*>(&element)->~T();
        }
    };

    template <class T>
    class LinkedVector {
    public:
        LinkedVector();
        explicit LinkedVector(T&&);

        explicit LinkedVector(const LinkedVector<T>&) = delete;
        LinkedVector<T>& operator=(const LinkedVector<T>&) = delete;

        Node<T>& operator[](int);
        Node<T>& front();
        Node<T>& back();

        int vector_size() const;  // size of the plain vector
        int queue_size() const;   // number of stored elements, e.g., txns
        int capacity() const;     // reserved physical size
        bool contains(int);  // check if a position is empty (removed)
        bool push_back(Node<T>&&);
        bool remove(int);
        bool empty() const;
        void clear();

    private:
        const int INITIAL_SIZE = 100;   // initial reversed # of slots
        // a vector of Nodes
        std::vector<Node<T>> nodes;
        // point to the head of the list of free nodes
        // free nodes re-use the vector positions
        int free_head;
        int _front;
        int _back;
        int _queue_size;
    };

    /*  Definitions of template LinkedVector class below */

    template <class T>
    LinkedVector<T>::LinkedVector() : nodes(), free_head(-1), _front(-1), _back(-1), _queue_size(0) {
        // default constructor
        nodes.reserve(INITIAL_SIZE);
    }

    template <class T>
    LinkedVector<T>::LinkedVector(T&& x) : free_head(-1), _front(0), _back(0), _queue_size(1) {
        nodes.reserve(INITIAL_SIZE);
        nodes.push_back(Node<T>(std::move(x)));
    }

    template <class T>
    Node<T>& LinkedVector<T>::operator[](int index) {
        assert(this->contains(index));
        return nodes.at(index);
    }

    template <class T>
    Node<T>& LinkedVector<T>::front() {
        assert(_front != -1);
        return nodes.at(_front);
    }

    template <class T>
    Node<T>& LinkedVector<T>::back() {
        assert(_back != -1);
        return nodes.at(_back);
    }

    template <class T>
    int LinkedVector<T>::vector_size() const {
        return nodes.size();
    }

    template <class T>
    int LinkedVector<T>::queue_size() const {
        return _queue_size;
    }

    template <class T>
    int LinkedVector<T>::capacity() const {
        return nodes.capacity();
    }

    template <class T>
    bool LinkedVector<T>::contains(int index) {
        return !nodes.at(index).free;
    }

    template <class T>
    bool LinkedVector<T>::push_back(Node<T>&& node) {
        if (free_head == -1) {
            // no prev removed nodes, no free positions
            nodes.push_back(std::move(node));   // use std::move, avoid copy
            nodes[_back]._next = nodes.size() - 1;
            nodes.back()._prev = _back;
            nodes.back()._pos = _back = nodes.size() - 1;
        } else {
            // there is free slot
            int free_slot = free_head;
            free_head = nodes[free_slot]._next;
            nodes[free_slot] = std::move(node);
            nodes[_back]._next = free_slot;
            nodes[free_slot]._prev = _back;
            nodes[free_slot]._pos = free_slot;
            _back = free_slot;
        }
        if (++_queue_size == 1) {
            // update front
            _front = _back;
        }
        return true;
    }

    template <class T>
    bool LinkedVector<T>::remove(int index) {
        if (nodes.size() <= index || nodes[index].free) {
            // out of range or position has been removed
            return false;
        }
        if (nodes[index]._prev != -1) {
            // this is not the head
            nodes[nodes[index]._prev]._next = nodes[index]._next;
            if (_back == index) {
                _back = nodes[index]._prev;
            }
        }
        if (nodes[index]._next != -1) {
            // this is not the tail
            nodes[nodes[index]._next]._prev = nodes[index]._prev;
            if (_front == index) {
                _front = nodes[index]._next;
            }
        }
        nodes[index]._next = free_head;
        nodes[index]._prev = -1;
        nodes[index]._pos = -1;
        nodes[index].free = true;
        free_head = index;
        if (--_queue_size == 0) {
            _front = _back = -1;
        }
        return true;
    }

    template <class T>
    bool LinkedVector<T>::empty() const {
        return _queue_size == 0;
    }

    template <class T>
    void LinkedVector<T>::clear() {
        nodes.clear();
        free_head = -1;
        _front = -1;
        _back = -1;
        _queue_size = 0;
    }

}