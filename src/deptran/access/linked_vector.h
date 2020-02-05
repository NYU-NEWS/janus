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
    // Define a node in the linked vector
    template <class T>
    struct Node {
        // the element in TxnQueue, it will be a txn struct
        typename std::aligned_storage<sizeof(T), alignof(T)>::type element;
        int next;   // point to next queued txn
        int prev;   // point to previous txn
        bool free;  // if this node position is free
        Node() : next(-1), prev(-1), free(false) {}  // a dangling node points to -1
        ~Node() {
            // destructor
            reinterpret_cast<T*>(&element)->~T();
        }
    };

    // Define iterators for the linked vector
    template <class T>
    class NodeIterator {
    public:

    private:
        std::vector<Node<T>>* nodes;
        int index;
    };

    // Define the linked vector
    template <class T>
    class LinkedVector {
    public:
        LinkedVector();
        LinkedVector(int);
        LinkedVector(const LinkedVector&);
        LinkedVector& operator=(const LinkedVector&);
        ~LinkedVector();

        int front();
        int back();
        int vector_size();  // size of the plain vector
        int queue_size();   // number of stored elements, e.g., txns
        bool contains(int);  // check if a position is empty (removed)
        bool push_back(Node<T>);
        bool remove(int);
        bool clear();

    private:
        // a vector of Nodes
        std::vector<Node<T>> nodes;
        // point to the head of the list of free nodes
        // free nodes re-use the vector positions
        int free_head;
        int _front;
        int _back;
        int _queue_size;
    };
}