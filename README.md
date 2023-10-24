### B+ Tree in Kotlin

This repo contains an implementation of a generic B+ tree data structure in Kotlin. Keys must be `Comparable`, and values can be whatever you want.

There are two slightly odd things about the implementation:
1. It's a completely in-memory implementaion.
2. It stores sets of data at the leaves, so that you can have multiple values for a given key.

If you're studying data structures, you'll read that B+ trees are mostly good for disk-based access. According to [the Wikipedia aricle on the topic](https://en.wikipedia.org/wiki/B%2B_tree), for example:

> The primary value of a B+ tree is in storing data for efficient retrieval in a block-oriented storage context — in particular, filesystems.

However, the B+ tree does seem to have some utility as an in-memory construct. In a case where you want to iterate over a range of keys (or the entire set of keys) quickly in a way that's maintained over insertions and deletions, it's a pretty good structure.

*NOTE*: This is a work in progress. I'm still implementing deletions and a ranged iterator.