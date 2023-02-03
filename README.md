[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.mgunlogson/cuckoofilter4j/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.mgunlogson/cuckoofilter4j)
[![Javadocs](http://javadoc.io/badge/com.github.mgunlogson/cuckoofilter4j.svg)](http://javadoc.io/doc/com.github.mgunlogson/cuckoofilter4j)
[![Build Status](https://travis-ci.org/MGunlogson/CuckooFilter4J.svg?branch=master)](https://travis-ci.org/MGunlogson/CuckooFilter4J)
[![Coverage Status](https://coveralls.io/repos/github/MGunlogson/CuckooFilter4J/badge.svg?branch=master)](https://coveralls.io/github/MGunlogson/CuckooFilter4J?branch=master)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

# This repo is unmaintained!
Sorry to users, I long since switched to using Bloom filters and don't currently have time to maintain this.

**There are known bugs in the implementation**, and many unrealized performance opportunities using primitives, new hashing algorithms, and possibly off-heap storage in Java 17+ . I definitely don't recommend using this in production, but leaving it up for existing users (and because I can't find another Java Cuckoo filter implementation). Feel free to use this as a base for your own improvements, and I will considering transfering ownership to a new maintainer if you agree to keep it fully open source under the same permissive license.


Cuckoo Filter For Java
==============
This library offers a similar interface to Guava's Bloom filters. In most cases it can be used interchangeably and has additional advantages including thread-safety, concurrent operations, deletions/counting and a configurable hashing algorithm.

This Forked version from MGunlogson/CuckooFilter4J has replaced the MurMur hash functions with xxHash which is much quicker. If you are only using the filter with primitive numbers (long,int,short,byte) you should use the filter from the primitive branch (https://github.com/MPdaedalus/CuckooFilter4J/tree/Primitive-Filter) as it is much quicker due to mightContain() calls creating no Garbage compared to this branch that supports Objects.

 * [About Cuckoo Filters](#about-cuckoo-filters)
 * [Installation](#installation)
 * [Usage](#usage)
 * [Documentation](#documentation)
   * [Deletions/Duplicates](#duplicates)
   * [Counting](#counting)
   * [Capacity](#capacity)
   * [Speed](#speed)
   * [Hashing Algorithms](#hashing-algorithms)
   * [Multi-Threading](#multi-threading)


About Cuckoo Filters
======

Cuckoo filter is a Bloom filter replacement for approximated set-membership queries. While Bloom filters are well-known space-efficient data structures to serve queries like "if item x is in a set?", they do not support deletion. Their variances to enable deletion (like counting Bloom filters) usually require much more space.

Cuckoo ﬁlters provide the ﬂexibility to add and remove items dynamically. A cuckoo filter is based on cuckoo hashing (and therefore named as cuckoo filter). It is essentially a cuckoo hash table storing each key's fingerprint. Cuckoo hash tables can be highly compact, thus a cuckoo filter could use less space than conventional Bloom ﬁlters, for applications that require low false positive rates (< 3%).

For details about the algorithm and citations please use this article for now

["Cuckoo Filter: Better Than Bloom" by Bin Fan, Dave Andersen and Michael Kaminsky](https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf)

If you're looking for an implementation in a different language check out these GitHub links.

* [C++ Implementation](https://github.com/efficient/cuckoofilter)
* [Golang Implementation](https://github.com/seiflotfy/cuckoofilter)

My personal thanks goes out to the authors of those libaries, their code helped immensely while writing my own implementation.

Installation
======



Maven artifact:
```xml
<dependency>
    <groupId>com.github.mgunlogson</groupId>
    <artifactId>cuckoofilter4j</artifactId>
    <version>1.0.1</version>
</dependency> 
```
<strong>[Download At Maven Central](http://search.maven.org/#artifactdetails%7Ccom.github.mgunlogson%7Ccuckoofilter4j%7C1.0.1%7Cjar)</strong>

Is performance important to your application?
------------------------
The regular filter is fairly quick, but [MPdaedalus](https://github.com/MPdaedalus) has made significant performance improvement by adding a brand new hash algorithm and reducing GC pressure. These changes speed up most operations by ~2x! His changes can be found [here](https://github.com/MPdaedalus/CuckooFilter4J). These improvements will be merged to version 2.0 as soon as the public API is finalized.


Usage
======
Below is a full example of creating and using the filter. Many more examples can be  found in the [test](/src/test/java/com/github/mgunlogson/cuckoofilter4j) folders within the project.

```java
import com.github.mgunlogson.cuckoofilter4j;
import com.google.common.hash.Funnels;

public class Example {

	public static void main(String[] args) {
		// create
		CuckooFilter<Integer> filter = new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000).build();
		// insert
		if (filter.put(42)) {
			System.out.println("Insert Success!");
		}
		// contains
		if (filter.mightContain(42)) {
			System.out.println("Found 42!");
		}
		// count
		System.out.println("Filter has " + filter.getCount() + " items");
		
				// count
		System.out.println("42 has been inserted approximately " + filter.approximateCount(42) + " times");

		// % loaded
		System.out.println("Filter is " + String.format("%.0f%%", filter.getLoadFactor() * 100) + " loaded");

		// delete
		if (filter.delete(42)) {
			System.out.println("Delete Success!");
		}
	}

}

```

Documentation
======
<strong>[Visit The JavaDoc Here](http://javadoc.io/doc/com.github.mgunlogson/cuckoofilter4j)</strong>

###False Positives
 The false positive rate of the filter is the probability that `mightContain()` will erroneously return `true` for an object that was not added to the filter. Unlike Bloom filters, a Cuckoo filter will fail to insert when it reaches capacity. If an insert fails `put()` will `return false`

Duplicates
----------------
Cuckoo filters allow deletion like counting Bloom filters. While counting Bloom filters invariably use more space to allow deletions, Cuckoo filters achieve this with *no* space or time cost. Like counting variations of Bloom filters, Cuckoo filters have a limit to the number of times you can insert duplicate items. This limit is 8-9 in the current design, depending on internal state. **Reaching this limit can cause further inserts to fail and degrades the performance of the filter**. Occasional duplicates will not degrade the performance of the filter but will slightly reduce capacity. Existing items can be deleted without affecting the false positive rate or causing false negatives. However, deleting items that were *not* previously added to the filter can cause false negatives.

Counting
----------------
Cuckoo filters support counting items, like counting Bloom filters. The maximum count is still limited by max-duplicates to 7 so this should only be used to count small numbers. The measured count may be higher than actual count due to false positives, but will never be lower since Cuckoo filters have no false negatives.

Capacity
----------------
Once the filter reaches capacity (`put()` returns false). It's best to either rebuild the existing filter or create a larger one. Deleting items in the current filter is also an option, but you should delete at least ~2% of the items in the filter before inserting again.

Speed
----------------
CuckooFilter4J is roughly the same speed as Guava's Bloom filters when running single-threaded. Guava's Bloom is usually faster with small tables, but the trend is reversed with tables too large to fit in the CPU cache. Overall the single-threaded speed of the two libraries is comparable. This library supports concurrent access through multithreading (Guava's Bloom does not). In my tests this scales fairly well, making CuckooFilter4J faster than Bloom filters for multi-threaded applications. On my 4 core machine, running inserts on all cores is roughly 3x faster than single-threaded operation. Cpu architecture will affect this, so your mileage may vary. See the [benchmark](bench/) folder for some tests to run on your own system.


Hashing Algorithms
----------------
Hash collision attacks are theoretically possible against Cuckoo filters (as with any hash table based structure). If this is an issue for your application, use one of the cryptographically secure (but slower) hash functions. The default hash function, Murmer3 is *not* secure. Secure functions include SHA and SipHash. All hashes,including non-secure, are internally seeded and salted. Practical attacks against any of them are unlikely. Also note that the maximum supported size of the filter depends on the hash funciton. Especially in the case of 32 bit Murmur3, the hash will limit table size. Even with a 32 bit hash, the maximum table size is around 270 megabytes. With 64 bit hashes the maximum table size is extremely large, and practically unlimited using 128+bit hash functions. In any case, the library will refuse to create the table using an invalid configuration.

Multi-Threading
----------------
All operations are thread-safe. Most also run concurrently for increased performance. Notable exceptions include copy, serialization, and hashcode which nessecarily lock the entire table - running on a single thread until complete. <strong>Thread safety should be considered BETA at the moment.</strong> Multithreading is notoriously hard to test, and despite my best effort to avoid bugs and deadlocks it is likely that some remain. If you are using multithreading in production I will do my best to provide prompt support and give you my thanks :).

Serializing
--------------------------------
Cuckoo filters are serializable.
