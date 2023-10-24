package com.dimdarkevil.bptree

import java.lang.RuntimeException

class BpTree<K: Comparable<K>, V> : Sequence<KeyVal<K,V>> {
    private val half = N / 2
    private var root: Node<K,V> = LeafNode()

    companion object {
        const val N: Int = 4
    }

    fun find(key: K): Set<V>? {
        val leaf = findLeafForKey(key)
        val existingIdx = leaf.keys.indexOfFirst { it == key }
        return if (existingIdx >= 0) {
            leaf.values[existingIdx]
        } else {
            null
        }
    }

    fun insert(key: K, value: V) {
        val leaf = findLeafForKey(key)
        // If the key already exists at the leaf, just insert the new value and return.
        val existingIdx = leaf.keys.indexOfFirst { it == key }
        if (existingIdx >= 0) {
            leaf.values[existingIdx].add(value)
            return
        }

        // There are only three cases when inserting a new key into a b+tree:
        // 1. the leaf is not full
        // 2. the leaf is full but its parent node is not.
        // 3. the leaf is full and its parent is too.
        if (!isFull(leaf)) {
            // case #1 - leaf not full
            addKeyValueToUnfullLeaf(leaf, key, value)
        } else if (leaf.parent?.let { !isFull(it) } != false) {
            // case #2 - leaf full, parent not full (or doesn't exist)

            // If we're here and the leaf has no parent, it means we're adding the
            // n+1th key in the entire list. We need to add a new parent and make
            // it the root.
            if (leaf.parent == null) {
                //println("-=-= new root from ${leaf.id} (unfull) at key $key")
                val r = InternalNode<K,V>()
                root = r
                // we set the leftmost pointer to the existing leaf (which used
                // to be the root) because when we add the new leaf (by splitting
                // the old one) it `will only set the right pointer.
                r.ptrs[0] = leaf
                leaf.parent = r
            }
            // Split the full leaf and insert the new key.
            // Half of the keys will be in the new leaf
            // and they will both have the same parent node.
            //println("-=-= split leaf (unfull) at key $key")
            val newLeaf = splitLeaf(leaf, key, value)

            // Add the leftmost key in the new leaf (aka the "middle key") to the
            // parent node's (unfull) list of keys.
            addKeyToUnfullNode(leaf.parent!!, newLeaf.keys[0]!!, newLeaf)
        } else {
            // case #3 - both leaf and its parent are full

            // Split the full leaf and insert the new key.
            // Half of the keys will be in the new leaf
            // and they will both have the same parent node.
            //println("-=-= split leaf ${leaf.id} (full) at key $key")
            val newLeaf = splitLeaf(leaf, key, value)
            var carriedKey =  newLeaf.keys[0]!!
            var newNode: Node<K,V> = newLeaf

            // This is the hard part. We will walk up the tree, splitting
            // full nodes as we go and carrying the "middle" key up to the
            // next level until either we find a non-full node or we hit
            // the top of the tree. In the latter case, we will need to
            // add a new root.
            var node: Node<K,V> = leaf
            while (node.parent != null && isFull(node.parent!!)) {
                val oldNode = node.parent!!
                //println("-=-= split leaf ${oldNode.id} (full) at key $key")
                val pair = splitNode(oldNode, carriedKey, newNode)
                newNode = pair.first
                carriedKey = pair.second
                // If the node we just split (oldNode) is the parent of leaves,
                // we need to set the new parent on the leaves that moved to it.
                if (node is LeafNode) {
                    newNode.ptrs.forEach {
                        if (it != null) {
                            val childLeaf = it
                            childLeaf.parent = newNode
                        }
                    }
                }
                node = oldNode
            }
            // If, at the end of walking up, the last node is full
            // and has no parent, we need to add a new root node.
            // Otherwise, the parent is not full and we just insert the key.
            if (node.parent == null) {
                //println("-=-= new root from ${leaf.id} (unfull) at key $key")
                val r = InternalNode<K,V>()
                root = r
                r.ptrs[0] = node
                node.parent = r
                newNode.parent = r
            }
            val nodeParent = node.parent!!
            addKeyToUnfullNode(nodeParent, carriedKey, newNode)
        }
    }


    // In a node, we have a list of n keys and n+1 pointers to other nodes.
    // So interleaving them looks like:
    //
    // ptr[0] | key[0] | ptr[1] | key[1] | ... | ptr[n] | key[n] | ptr[n+1]
    //
    // This gives us a nice structure where the pointer at any index i is the pointer to the node
    // "left of" the key at index i (meaning that the node pointed to has all keys less than the
    // key at i) and the pointer at index i+1 is the pointer to the node "right of" the key at index
    // i (meaning that the node pointed to has all keys greater than or equal to the key at i).
    //
    // Leaf pointers don't need this interleaved structure because they don't point to other
    // nodes, but they *do* need to be able to point to the next and previous leaves in sorted
    // order, so that we can iterate the whole set in either order. We'll use ptr[0] as a
    // "previous leaf" pointer and ptr[n] as a "next leaf" pointer for leaf nodes in this
    // implementation. The pointers will be maintained as we add new leaf nodes.
    //
    // This function finds the leaf in which a key should appear if it exists.
    // You'd have to iterate over the keys in this leaf node to see if it really does.
    private fun findLeafForKey(key: K) : LeafNode<K,V> {
        var node: Node<K,V> = root
        while (node is InternalNode) {
            // Get the index of the first key in the node that is either greater than the
            // supplied key or null.  If it exists, this will be the index of the pointer left
            // of that key. If it doesn't exist, that means the list is full and the rightmost
            // key is less than the supplied key.
            //
            // case 1: found a key greater or equal
            // ptr0 | key0 (2) | ptr1 | key1 (4) | ptr2 | key2 (6) | ptr3 | key3 (8) | ptr4
            // looking for key 3 finds index 1
            //
            // case 2: found a null key
            // ptr0 | key0 (2) | ptr1 | key1 (4) | ptr2 | key2 (null) | ptr3 | key3 (null) | ptr4
            // looking for key 5 finds index 2
            //
            // case 3: no null or greater key
            // ptr0 | key0 (2) | ptr1 | key1 (4) | ptr2 | key2 (6) | ptr3 | key3 (8) | ptr4
            // looking for key 9 finds index -1
            //
            val i = node.keys.indexOfFirst { it == null || it > key }
            node = if (i < 0) {
                // If we didn't find a key that was greater or null, we know the list is
                // full and doesn't contain a key greater than or equal to the supplied key.
                // So we return the rightmost pointer.
                node.ptrs[N] ?: throw RuntimeException("ptr at $N is null")
            } else {
                // We found a key that was greater or null, so we want to navigate down to the pointer
                // left of that key.
                node.ptrs[i] ?: throw RuntimeException("ptr at $i is null")
            }
        }
        return node as LeafNode<K, V>
    }

    // This function splits a non-leaf node, inserts a new key,
    // pulls out the "middle" key, and returns both this middle
    // key and the newly created right-hand node.
    //
    // Splitting a node is actually an insert and split operation.
    // The bad news is that the node is full, so we can't just
    // insert the new key before we do the split. The worse news
    // is that we want the first key in the new node (after the split)
    // to be the "middle" node in sort order, so we can't do the
    // split until we've inserted the new key. Chicken and egg.
    //
    // Since we want to avoid temporary storage, and we don't want
    // to allocate two new nodes if we can avoid it, we're going
    // to allocate one new node, put the overflowing value there,
    // and then shift over the values from the old node, inserting
    // the new key at the correct spot along the way.
    //
    // This function should only be called with a full node.
    private fun splitNode(node: InternalNode<K,V>, newKey: K, newPtr: Node<K,V>) : Pair<InternalNode<K,V>,K> {
        // Split the node in half by creating a new node and moving the
        // right half of the old node's keys to it.
        val newNode = InternalNode<K,V>()
        newNode.parent = node.parent

        // The new leaf is going to have (n/2) + 1 keys, and
        // the old leaf is going to have (n/2) keys.
        //
        // The following process moves from right to left
        // starting with the new node, then getting the "middle"
        // key, then handling the left node. Moving in this
        // direction lets us fill things in non-destructively.
        var srcIdx = N-1
        var inserted = false
        (half-1 downTo 0).forEach {
            // If the key at srcIdx is less than the new key, we
            // insert the new key. Otherwise we insert the key at
            // srcIdx. We track "inserted" so we don't try to insert
            // the new key more than once.
            if (!inserted && node.keys[srcIdx]!! <= newKey) {
                newNode.keys[it] = newKey
                newNode.ptrs[it+1] = newPtr
                inserted = true
            } else {
                newNode.keys[it] = node.keys[srcIdx]
                newNode.ptrs[it+1] = node.ptrs[srcIdx+1]
                srcIdx--
            }
        }
        // Now we need to pull the "middle" value, whose right-hand
        // pointer will be the new node we added. The middle key
        // will end up getting pushed up the tree to the next higher
        // node.
        val middleKey = if (!inserted && node.keys[srcIdx]!! <= newKey) {
            newNode.ptrs[0] = newPtr
            inserted = true
            newKey
        } else {
            val m = node.keys[srcIdx]!!
            newNode.ptrs[0] = node.ptrs[srcIdx+1]
            srcIdx--
            m
        }
        // If we already inserted the new key, the old node doesn't need
        // to be shifted. Otherwise we do need to shift. It's basically
        // the same process as we did with the new node
        if (!inserted) {
            ((half-1) downTo 0).forEach {
                if (!inserted && (srcIdx < 0 || node.keys[srcIdx]!! <= newKey)) {
                    node.keys[it] = newKey
                    node.ptrs[it+1] = newPtr
                    inserted = true
                } else {
                    node.keys[it] = node.keys[srcIdx]
                    node.ptrs[it+1] = node.ptrs[srcIdx+1]
                    srcIdx--
                }
            }
        }
        // Now we need to clear out the keys in the right half of the
        // old node at indices of half or above (since we moved them
        // to the new node)
        (half..<N).forEach {
            node.keys[it] = null
            node.ptrs[it+1] = null
        }

        (0..<N).forEach {
            newNode.ptrs[it]?.let { nc ->
                if (nc.parent != newNode) {
                    //println("-=-= changing parent of node ${nc.id} from ${nc.parent} to ${newNode.id}")
                    nc.parent = newNode
                }
            }
        }

        return Pair(newNode, middleKey)
    }

    // This function splits a leaf node, inserts a new key
    // at the appropriate location and returns the new
    // leaf added to the right-hand side.
    //
    // See splitNode() for a description of how we're going
    // to go about splitting the leaf. It's a little easier
    // to split a leaf because we don't have to worry about
    // keeping pointers in the right place. Also leaf nodes
    // don't have the middle key removed when split.
    //
    // This function should only be called with a full leaf.
    private fun splitLeaf(leaf: LeafNode<K,V>, newKey: K, newValue: V) : LeafNode<K,V> {
        // Split the leaf in half by creating a new leaf and moving the
        // right half of the old leaf's keys to it.
        val newLeaf = LeafNode<K,V>()
        newLeaf.parent = leaf.parent

        // The new leaf is going to have (n/2) + 1 keys, and
        // the old leaf is going to have (n/2) keys.
        //
        // The following process moves from right to left
        // starting with the new leaf, then handling the old leaf.
        // Moving in this direction lets us fill things in non-destructively.
        var srcIdx = N-1
        var inserted = false
        // first fill up the new node
        (half downTo 0).forEach {
            // If the key at srcIdx is less than the new key, we
            // insert the new key. Otherwise we insert the key at
            // srcIdx. We track "inserted" so we don't try to insert
            // the new key more than once.
            if (!inserted && leaf.keys[srcIdx]!! <= newKey) {
                newLeaf.keys[it] = newKey
                newLeaf.values[it] = mutableSetOf(newValue)
                inserted = true
            } else {
                newLeaf.keys[it] = leaf.keys[srcIdx]
                newLeaf.values[it] = leaf.values[srcIdx]
                srcIdx--
            }
        }
        // If we already inserted the new key, the old leaf doesn't need
        // to be shifted. Otherwise we do need to shift. It's basically
        // the same process as we did with the new leaf
        if (!inserted) {
            ((half-1) downTo 0).forEach {
                if (!inserted && (srcIdx < 0 || leaf.keys[srcIdx]!! <= newKey)) {
                    leaf.keys[it] = newKey
                    leaf.values[it] = mutableSetOf(newValue)
                    inserted = true
                } else {
                    leaf.keys[it] = leaf.keys[srcIdx]
                    leaf.values[it] = leaf.values[srcIdx]
                    srcIdx--
                }
            }
        }
        // Now we need to clear out the keys in the old leaf at indices of
        // half or above (since we moved them to the new leaf)
        (half..<N).forEach {
            leaf.keys[it] = null
            leaf.values[it] = mutableSetOf()
        }

        // Move the old leaf's "next leaf" pointer to be
        // the new leaf's "next leaf" pointer.
        newLeaf.nextPtr = leaf.nextPtr
        // Set the old leaf's "next leaf" pointer to
        // point to the new leaf.
        leaf.nextPtr = newLeaf
        // Set the new leaf's "previous leaf" pointer to
        // point to the old leaf.
        newLeaf.prevPtr = leaf
        // If there is a next leaf, set its "previous leaf"
        // pointer to the new leaf.
        newLeaf.nextPtr?.let { nextLeaf ->
            nextLeaf.prevPtr = newLeaf
        }
        return newLeaf
    }

    // This inserts a key and its new right node at the correct place
    // in a non-leaf node that's not full. You only want to call this if you
    // know for sure that the node isn't full, because if it's full, this
    // function will either drop a key or throw an exception.
    private fun addKeyToUnfullNode(node: InternalNode<K,V>, key: K, rightPtr: Node<K,V>) {
        val i = node.keys.indexOfFirst { it == null || it > key }
        if (i < 0) throw RuntimeException("Attempting to insert into a full node")
        // shift the keys that are greater (and their pointers) to the right if needed
        if (node.keys[i] != null) {
            (N-1 downTo i+1).forEach {
                node.keys[it] = node.keys[it-1]
                node.ptrs[it+1] = node.ptrs[it]
            }
        }
        node.keys[i] = key
        node.ptrs[i+1] = rightPtr
    }

    // This inserts a key at the correct place in a leaf node that's not full.
    // You only want to call this if you know for sure that the leaf isn't full,
    // because if it's full, this function will either drop a key or throw an exception.
    private fun addKeyValueToUnfullLeaf(node: LeafNode<K,V>, key: K, value: V) {
        val i = node.keys.indexOfFirst { it == null || it > key }
        if (i < 0) throw RuntimeException("Attempting to insert into a full node")
        // shift the keys that are greater (and their pointers) to the right if needed
        if (node.keys[i] != null) {
            (N-1 downTo i+1).forEach {
                node.keys[it] = node.keys[it-1]
                node.values[it] = node.values[it-1]
            }
        }
        node.keys[i] = key
        node.values[i] = mutableSetOf(value)
    }

    // Returns true if a node is full. False otherwise.
    private fun isFull(node: Node<K,V>) = (node.keys[N-1] != null)

    private fun getLeftmostLeaf() : LeafNode<K,V> {
        var node: Node<K,V> = root
        while (node is InternalNode) {
            node = node.ptrs[0]!!
        }
        return node as LeafNode<K, V>
    }

    private fun getRightmostLeaf() : LeafNode<K,V> {
        var node: Node<K,V> = root
        while (node is InternalNode) {
            node = node.ptrs.last { it != null }!!
        }
        return node as LeafNode<K, V>
    }

    /**
     * A little iterator class that exists because we want to be able to use
     * the index as a lazily-iterated sequence.
     */
    private class AscendingIterator<K: Comparable<K>,V>(val tree: BpTree<K,V>) : Iterator<KeyVal<K,V>> {
        private var leaf: LeafNode<K,V>? = tree.getLeftmostLeaf()
        private var idx = -1

        init {
            advance()
        }

        private fun advance() {
            if (leaf == null) return
            if (idx >= N) {
                idx = -1
                leaf = leaf!!.nextPtr
            }
            if (leaf == null) return
            idx++
            while (idx < N && leaf!!.keys[idx] != null) idx++
            if (idx >= N) {
                advance()
            }
        }

        override fun hasNext(): Boolean {
            return leaf != null
        }

        override fun next(): KeyVal<K,V> {
            if (leaf == null) throw NoSuchElementException("advanced past end of iterator")
            val retKey = leaf!!.keys[idx]!!
            val retVals = leaf!!.values[idx]
            advance()
            return KeyVal(retKey, retVals)
        }
    }

    /**
     * Get an iterator over the sorted key IDs in the index
     */
    override fun iterator(): Iterator<KeyVal<K,V>> = AscendingIterator(this)

    fun forEachAscending(func: (K, Set<V>) -> Unit) {
        var leaf : LeafNode<K,V>? = getLeftmostLeaf()
        while (leaf != null) {
            leaf.keys.forEachIndexed { idx, key ->
                if (key != null) func(key, leaf!!.values[idx])
            }
            leaf = leaf.nextPtr
        }
    }

    fun forEachDescending(func: (K, Set<V>) -> Unit) {
        var leaf : LeafNode<K,V>? = getRightmostLeaf()
        while (leaf != null) {
            (N-1 downTo 0).forEach { idx ->
                leaf!!.keys[idx]?.let { key ->
                    func(key, leaf!!.values[idx])
                }
            }
            leaf = leaf.prevPtr
        }
    }
}

sealed interface Node<K:Comparable<K>,V> {
    var parent: InternalNode<K,V>?
    var numKeys: Int
    val keys: Array<K?>
}

// keys and pointers are actually interleaved, like
// | ptr | key | ptr | key | ptr |
// so a key always has a left and right pointer.
// the left pointer has the same index as the key,
// and the right pointer is at the key's index + 1
@Suppress("UNCHECKED_CAST")
class InternalNode<K:Comparable<K>,V>(
    override var parent: InternalNode<K,V>? = null,
    override var numKeys: Int = 0,
    override val keys: Array<K?> = arrayOfNulls<Comparable<K>?>(BpTree.N) as Array<K?>,
    val ptrs: Array<Node<K,V>?> = arrayOfNulls(BpTree.N+1),
): Node<K,V>

@Suppress("UNCHECKED_CAST")
class LeafNode<K:Comparable<K>,V>(
    override var parent: InternalNode<K,V>? = null,
    override var numKeys: Int = 0,
    override val keys: Array<K?> = arrayOfNulls<Comparable<K>?>(BpTree.N) as Array<K?>,
    val values: Array<MutableSet<V>> = Array(BpTree.N) { mutableSetOf() },
    var prevPtr: LeafNode<K,V>? = null,
    var nextPtr: LeafNode<K,V>? = null,
): Node<K,V>

data class KeyVal<K, V>(
    val key: K,
    val v: Set<V>
)
