Add support for Lemire's fastrange.
-----------
This involves removing 64 index support, but it doesn't shink the size of the filters you can use in practice because of other numerical limitations. Using fastrange will remove any divisions from table calculations and remove the need for table sizes to be a multiple of two.
https://github.com/lemire/fastrange

Add support for table sizes that aren't a multipe of two
-----------------
Serializing these filters is probably going to be done a lot in practice. They need to be as small as possible in RAM and on disk. Adding support for non-power-of-two tables sizes is an open ended problem in the Cuckoo filter papers. Practically, the indexes need to be switched from using modulo for offsets because it introduces biases when the table size isn't a power of two https://ericlippert.com/2013/12/16/how-much-bias-is-introduced-by-the-remainder-technique/ . This can be done with Lemire's fastrange modified to remove bias, or http://www.hackersdelight.org/magic.htm "magic" precalculation of the divisor.

The other hurdle for non multiple-of-two sizes is that it can break the XOR operation used to find the alternate index for a given fingerprint. Since XOR is done on a certain number of bits it will always produce an index up to that power of two. THis means when used on non power of two table sizes it can produce alternate indexes that are out of the valid range. For now, my approach is to discard such fingerprint-bucket combinations when they are created. This adds a small amount of overhead but it is unavoidable if we want to keep using XOR to calculate alternate buckets, and I can't think of another good solution so far. 


Modify code to avoid extraneous calculation of alternate index
-------------------
There are several places where we calculate the alternate bucket index even though it is only needed when running in multi-threaded mode to make sure the buckets are locked in the right order. Remove these calculations when running in single thread mode.


Optimize code that compares tags
-------------------------
right now we use a simple bitbanging technique to compare tags one bit at a time. For most tag lengths it is more efficient to shift the tags and compare all at once. Change logic to compare tags in a more optimal way.

Fix division by zero bug when bucket index is zero
--------------------------

Add method to calculate instantaneous estimated false positive rate
----------------------------

Add hook to callback for when filter is close to overflow
---------------------------------

Add function to clear the filter
-------------------
Need a quick mem wipe to zero out the filter.. possibly use unsafe methods?

Add support for xxHash
---------------
Guava's Bloom recently switched to a new hashing algorithm, XXHash, that gives a substanial increase in performance. The filter should be optimized to use this hash. MPdaedalus might be working on this

Improve bitset implementation
--------------
Current one was modified from Apache Lucene and isn't ideal for what we're doing with it.

Add support for custom serialization
------------------------
this will save a little space when serializing/deserializing.

Strip support for multiple hash functions and just use the fastest
---------------------------
in practice the demand seems to be for the fastest filter possible, configurability be damned

add more multithreaded tests
------------------------

Add statistical tests to catch insidious degredation of filter quality
-------------------

add tests to catch non-random bucket index and tag values
--------------------


add tests to catch invalid or missing bucket index and tag values
---------------------------

