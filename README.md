#A unit tested Cuckoo filter for Java, built using Guava.#


Code will be on maven etc... soon, interface is **NOT** finalized. I'm planning to ditch BitSet and replace it with my own implenetation to get beyond the 32 bit size bounary. 
========================


About
-----------------
Cuckoo filter is a Bloom filter replacement for approximated set-membership queries. While Bloom filters are well-known space-efficient data structures to serve queries like "if item x is in a set?", they do not support deletion. Their variances to enable deletion (like counting Bloom filters) usually require much more space.

Cuckoo ﬁlters provide the ﬂexibility to add and remove items dynamically. A cuckoo filter is based on cuckoo hashing (and therefore named as cuckoo filter). It is essentially a cuckoo hash table storing each key's fingerprint. Cuckoo hash tables can be highly compact, thus a cuckoo filter could use less space than conventional Bloom ﬁlters, for applications that require low false positive rates (< 3%).

For details about the algorithm and citations please use this article for now

["Cuckoo Filter: Better Than Bloom" by Bin Fan, Dave Andersen and Michael Kaminsky](https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf)

If you're looking for an implementation in a different language check out these GitHub links.

* [C++ Implementation](https://github.com/efficient/cuckoofilter)
* [Golang Implementation](https://github.com/seiflotfy/cuckoofilter)

My personal thanks goes out to the authors of those libaries, their code helped immensely while writing my own implementation.


Usage
-------------------

Below is a full example of creating and using the filter. Many more examples can be  found in the TEST(link) and BENCHMARK(link) sections of the project.

```java
import com.cuckooforjava.CuckooFilter;
import com.google.common.hash.Funnels;

public class Example {

	public static void main(String[] args) {
		// create
		CuckooFilter<Integer> filter = CuckooFilter.create(Funnels.integerFunnel(), 100000);
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
-------------------
**Visit the JavaDoc here**


False Positives
-----------------
 The false positive rate of the filter is the probability that `mightContain()` will erroneously return `true` for an object that was not added to the filter. Unlike Bloom filters, a Cuckoo filter will fail to insert when it reaches capacity. If an insert fails `put()` will `return false`

Deletions/Duplicates
-----------------
Cuckoo filters allow deletion like counting Bloom filters. While counting Bloom filters invariably use more space to allow deletions, Cuckoo filters achieve this with *no* space or time cost. Like counting variations of Bloom filters, Cuckoo filters have a limit to the number of times you can insert duplicate items. This limit is 8-9 in the current design, depending on internal state. **Reaching this limit can cause further inserts to fail and degrades the performance of the filter**. Occasional duplicates will not degrade the performance of the filter but will slightly reduce capacity. Existing items can be deleted without affecting the false positive rate or causing false negatives. However, deleting items that were *not* previously added to the filter can cause false negatives.

Capacity
-------------------- 
Once the filter reaches capacity (`put()` returns false). It's best to either rebuild the existing filter or create a larger one. Deleting items in the current filter is also an option, but you should delete at least ~2% of the items in the filter before inserting again.

Hashing Algorithms
----------------------------
Hash collision attacks are theoretically possible against Cuckoo filters (as with any hash table based structure). If this is an issue for your application, use one of the cryptographically secure (but slower) hash functions. The default hash function, Murmer3 is *not* secure. Secure functions include SHA and SipHash. All hashes,including non-secure, are internally seeded and salted. Practical attacks against any of them are unlikely.

Benchmark Results
------------------------------
NEED TO MAKE THIS STUFF


