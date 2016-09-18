/*
   Copyright 2016 Mark Gunlogson

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.TWARE.
*/

package com.cuckooforjava;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.math.RoundingMode;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Funnel;
import com.google.common.math.DoubleMath;

/**
 * A Cuckoo filter for instances of {@code T}. Cuckoo filters are probabilistic
 * hash tables similar to Bloom filters but with several advantages. Like Bloom
 * filters, a Cuckoo filter can determine if an object is contained within a set
 * at a specified false positive rate with no false negatives. Like Bloom, a
 * Cuckoo filter can determine if an element is probably inserted or definitely
 * is not. In addition, and unlike standard Bloom filters, Cuckoo filters allow
 * deletions. They also use less space than a Bloom filter for the same
 * performance.
 *
 * <p>
 * The false positive rate of the filter is the probability that
 * {@linkplain #mightContain(Object)}} will erroneously return {@code true} for
 * an object that was not added to the filter. Unlike Bloom filters, a Cuckoo
 * filter will fail to insert when it reaches capacity. If an insert fails
 * {@linkplain #put(Object)} will {@code return false} .
 * 
 * <p>
 * Cuckoo filters allow deletion like counting Bloom filters. While counting
 * Bloom filters invariably use more space to allow deletions, Cuckoo filters
 * achieve this with <i>no</i> space or time cost. Like counting variations of
 * Bloom filters, Cuckoo filters have a limit to the number of times you can
 * insert duplicate items. This limit is 8-9 in the current design, depending on
 * internal state. <i>Reaching this limit can cause further inserts to fail and
 * degrades the performance of the filter</i>. Occasional duplicates will not
 * degrade the performance of the filter but will slightly reduce capacity.
 * 
 * <p>
 * Once the filter reaches capacity ({@linkplain #put(Object)} returns false).
 * It's best to either rebuild the existing filter or create a larger one.
 * Deleting items in the current filter is also an option, but you should delete
 * at least ~2% of the items in the filter before inserting again.
 * 
 * <p>
 * Existing items can be deleted without affecting the false positive rate or
 * causing false negatives. However, deleting items that were <i>not</i>
 * previously added to the filter can cause false negatives.
 * 
 * <p>
 * Hash collision attacks are theoretically possible against Cuckoo filters (as
 * with any hash table based structure). If this is an issue for your
 * application, use one of the cryptographically secure (but slower) hash
 * functions. The default hash function, Murmer3 is <i>not</i> secure. Secure
 * functions include SHA and SipHash. All hashes,including non-secure, are
 * internally seeded and salted. Practical attacks against any of them are
 * unlikely.
 * 
 * <p>
 * This implementation of a Cuckoo filter is serializable.
 * 
 * 
 * @see <a href="https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf">
 *      paper on Cuckoo filter properties.</a>
 * @see <a href="https://github.com/seiflotfy/cuckoofilter">Golang Cuckoo filter
 *      implementation</a>
 * @see <a href="https://github.com/efficient/cuckoofilter">C++ reference
 *      implementation</a>
 *
 *
 * @param <T>
 *            the type of items that the {@code CuckooFilter} accepts
 * @author Mark Gunlogson
 */
public final class CuckooFilter<T> implements Serializable {

	private static final long serialVersionUID = -1337735144654851942L;
	static final int INSERT_ATTEMPTS = 500;
	static final int BUCKET_SIZE = 4;
	// make sure to update getNeededBitsForFpRate() if changing this... then
	// again don't change this
	static final double LOAD_FACTOR = 0.955;
	static final double DEFAULT_FP = 0.01;

	private FilterTable table;
	private IndexTagCalc<T> hasher;
	private long count;

	@VisibleForTesting
	Victim victim;
	@VisibleForTesting
	boolean hasVictim;

	/**
	 * The hashing algorithm used internally.
	 * 
	 * @author Mark Gunlogson
	 *
	 */
	public enum Algorithm {
		/**
		 * Murmer3 - 32 bit version, This is the default.
		 */
		Murmur3_32(0),
		/**
		 * Murmer3 - 128 bit version. Slower than 32 bit Murmer3, not sure why
		 * you would want to use this.
		 */
		Murmur3_128(1),
		/**
		 * SHA1 secure hash.
		 */
		sha1(2),
		/**
		 * SHA256 secure hash.
		 */
		sha256(2),
		/**
		 * SipHash(2,4) secure hash.
		 */
		sipHash24(3);
		private final int id;

		Algorithm(int id) {
			this.id = id;
		}

		public int getValue() {
			return id;
		}
	}

	/**
	 * when the filter becomes completely full, the last item that fails to be
	 * repositioned will be left without a home. We need to store it to avoid a
	 * false negative. Victim may be stale since we use flag to check if exists
	 */
	class Victim implements Serializable {
		private static final long serialVersionUID = -984233593241086192L;
		long i1;
		long i2;
		long tag;

		Victim(long bucketIndex, long tag) {
			this.i1 = bucketIndex;
			this.i2 = hasher.altIndex(bucketIndex, tag);
			this.tag = tag;
		}

		Victim() {

		}

		@Override
		public int hashCode() {
			return Objects.hash(i1, i2, tag);
		}

		@Override
		public boolean equals(@Nullable Object object) {
			if (object == this) {
				return true;
			}
			if (object instanceof CuckooFilter.Victim) {
				@SuppressWarnings("rawtypes") // don't care what parent class
												// generic type is
				CuckooFilter.Victim that = (CuckooFilter.Victim) object;
				return (this.i1 == that.i1 || this.i1 == that.i2) && this.tag == that.tag;
			}
			return false;
		}

		Victim copy() {
			return new Victim(i1, tag);
		}
	}

	/**
	 * Creates a Cuckoo filter.
	 */
	private CuckooFilter(IndexTagCalc<T> hasher, FilterTable table, long count, boolean hasVictim, Victim victim) {
		this.hasher = hasher;
		this.table = table;
		this.count = count;
		this.hasVictim = hasVictim;
		// no nulls even if victim hasn't been used!
		if (victim == null)
			this.victim = new Victim();
		else
			this.victim = victim;
	}

	/**
	 * Creates a {@link CuckooFilter CuckooFilter} for the expected number of
	 * insertions with a false positive rate of 1%
	 *
	 * <p>
	 * Note that overflowing a {@code CuckooFilter} with significantly more
	 * elements than specified will result in insertion failure.
	 *
	 * <p>
	 * The constructed {@code BloomFilter<T>} will be serializable if the
	 * provided {@code Funnel<T>} is.
	 *
	 * <p>
	 * It is recommended that the funnel be implemented as a Java enum. This has
	 * the benefit of ensuring proper serialization and deserialization, which
	 * is important since {@link #equals} also relies on object identity of
	 * funnels.
	 *
	 * @param <T>
	 *            the type of item filter holds
	 *
	 * @param funnel
	 *            the funnel of T's that the constructed {@code CuckooFilter<T>}
	 *            will use
	 * @param maxKeys
	 *            the number of expected insertions to the constructed
	 *            {@code CuckooFilter<T>}; must be positive
	 * 
	 * @return a {@code CuckooFilter}
	 */
	public static <T> CuckooFilter<T> create(Funnel<? super T> funnel, int maxKeys) {
		return create(funnel, maxKeys, DEFAULT_FP, Algorithm.Murmur3_32);
	}

	/**
	 * Creates a {@link CuckooFilter CuckooFilter} for the expected number of
	 * insertions and false positive rate
	 *
	 * <p>
	 * Note that overflowing a {@code CuckooFilter} with significantly more
	 * elements than specified will result in insertion failure.
	 *
	 * <p>
	 * The constructed {@code BloomFilter<T>} will be serializable if the
	 * provided {@code Funnel<T>} is.
	 *
	 * <p>
	 * It is recommended that the funnel be implemented as a Java enum. This has
	 * the benefit of ensuring proper serialization and deserialization, which
	 * is important since {@link #equals} also relies on object identity of
	 * funnels.
	 *
	 * @param <T>
	 *            the type of item filter holds
	 *
	 * @param funnel
	 *            the funnel of T's that the constructed {@code CuckooFilter<T>}
	 *            will use
	 * @param maxKeys
	 *            the number of expected insertions to the constructed
	 *            {@code CuckooFilter<T>}; must be positive
	 * @param fpp
	 *            the desired false positive probability (must be positive and
	 *            less than 1.0)
	 * 
	 * @return a {@code CuckooFilter}
	 */
	public static <T> CuckooFilter<T> create(Funnel<? super T> funnel, int maxKeys, double fpp) {
		return create(funnel, maxKeys, fpp, Algorithm.Murmur3_32);
	}

	/**
	 * Creates a {@link CuckooFilter CuckooFilter} for the expected number of
	 * insertions and false positive rate, using the specified hashing
	 * algorithm.
	 *
	 * <p>
	 * Note that overflowing a {@code CuckooFilter} with significantly more
	 * elements than specified will result in insertion failure.
	 *
	 * <p>
	 * The constructed {@code BloomFilter<T>} will be serializable if the
	 * provided {@code Funnel<T>} is.
	 *
	 * <p>
	 * It is recommended that the funnel be implemented as a Java enum. This has
	 * the benefit of ensuring proper serialization and deserialization, which
	 * is important since {@link #equals} also relies on object identity of
	 * funnels.
	 *
	 * @param <T>
	 *            the type of item filter holds
	 *
	 * @param funnel
	 *            the funnel of T's that the constructed {@code CuckooFilter<T>}
	 *            will use
	 * @param maxKeys
	 *            the number of expected insertions to the constructed
	 *            {@code CuckooFilter<T>}; must be positive
	 * 
	 * @param hashAlgorithm
	 *            the {@code Algorithm} to use for hashing items into the
	 *            filter.
	 * 
	 * @return a {@code CuckooFilter}
	 */
	public static <T> CuckooFilter<T> create(Funnel<? super T> funnel, int maxKeys, Algorithm hashAlgorithm) {
		return create(funnel, maxKeys, DEFAULT_FP, hashAlgorithm);
	}

	public static <T> CuckooFilter<T> create(Funnel<? super T> funnel, long maxKeys, double fpp,
			Algorithm hashAlgorithm) {
		checkArgument(maxKeys > 1, "maxKeys (%s) must be > 1, increase maxKeys", maxKeys);
		checkArgument(fpp > 0, "fpp (%s) must be > 0, increase fpp", fpp);
		checkArgument(fpp < .25, "fpp (%s) must be < 0.25, decrease fpp", fpp);
		checkNotNull(hashAlgorithm);
		checkNotNull(funnel);
		int tagBits = getBitsPerItemForFpRate(fpp);
		long numBuckets = getBucketsNeeded(maxKeys);
		IndexTagCalc<T> hasher = new IndexTagCalc<>(hashAlgorithm, funnel, numBuckets, tagBits);
		FilterTable filtertbl = FilterTable.create(tagBits, numBuckets, maxKeys);
		return new CuckooFilter<>(hasher, filtertbl, 0, false, null);

	}

	/**
	 * Calculates how many bits are needed to reach a given false positive rate.
	 * 
	 * @param fpProb
	 *            the false positive probability.
	 * @return the length of the tag needed (in bits) to reach the false
	 *         positive rate.
	 */
	private static int getBitsPerItemForFpRate(double fpProb) {
		/*
		 * equation from Cuckoo Filter: Practically Better Than Bloom Bin Fan,
		 * David G. Andersen, Michael Kaminsky , Michael D. Mitzenmacher
		 */
		return DoubleMath.roundToInt(DoubleMath.log2((1 / fpProb) + 3) / LOAD_FACTOR, RoundingMode.UP);
	}

	/**
	 * Calculates how many buckets are needed to hold the chosen number of keys,
	 * taking the standard load factor into account.
	 * 
	 * @param maxKeys
	 *            the number of keys the filter is expected to hold before
	 *            insertion failure.
	 * @return The number of buckets needed
	 */
	private static long getBucketsNeeded(long maxKeys) {
		/*
		 * force a power-of-two bucket count so hash functions for bucket index
		 * can hashBits%numBuckets and get randomly distributed index. See wiki
		 * "Modulo Bias". Only time we can get perfectly distributed index is
		 * when numBuckets is a power of 2.
		 */
		long bucketsNeeded = DoubleMath.roundToLong((1.0 / LOAD_FACTOR) * maxKeys / BUCKET_SIZE, RoundingMode.UP);
		// get next biggest power of 2
		long bitPos = Long.highestOneBit(bucketsNeeded);
		if (bucketsNeeded > bitPos)
			bitPos = bitPos << 1;
		return bitPos;
	}

	/**
	 * Gets the current number of items in the Cuckoo filter. Can be higher than
	 * the max number of keys the filter was created to store in some cases.
	 * 
	 * @return number of items in filter
	 */
	public long getCount() {
		// can return more than maxKeys if running above design limit!
		return count;
	}

	/**
	 * Gets the current load factor of the Cuckoo filter. Reasonably sized
	 * filters with randomly distributed values can be expected to reach a load
	 * factor of around 95% (0.95) before insertion failure
	 * 
	 * @return load fraction of total space used
	 */
	public double getLoadFactor() {
		return count / (hasher.getNumBuckets() * (double) BUCKET_SIZE);
	}

	/**
	 * Gets the size of the underlying {@code BitSet} table for the filter, in
	 * bits. This is <i>not</i> the actual size of the filter in memory.
	 * 
	 * @return space used by table in bits
	 */
	long getStorageSize() {
		return table.getStorageSize();
	}

	/**
	 * Puts an element into this {@code CuckooFilter}. Ensures that subsequent
	 * invocations of {@link #mightContain(Object)} with the same element will
	 * always return {@code true}.
	 * <p>
	 * Note that the filter should be considered full after insertion failure.
	 * Further inserts <i>may</i> fail, although deleting items can also make
	 * the filter usable again.
	 * <p>
	 * Also note that inserting the same item more than 8 times will cause an
	 * insertion failure.
	 *
	 * @param item
	 *            item to insert into the filter
	 *
	 * @return {@code true} if the cuckoo filter inserts this item successfully.
	 *         Returns {@code false} if insertion failed.
	 */
	public boolean put(T item) {
		BucketAndTag pos = hasher.generate(item);
		long curTag = pos.tag;
		long curIndex = pos.index;
		long altIndex = hasher.altIndex(curIndex, curTag);
		if (table.insertToBucket(curIndex, curTag) || table.insertToBucket(altIndex, curTag)) {
			count++;
			return true;
		}
		// don't do insertion loop if victim slot is already filled
		if (hasVictim)
			return false;
		// if we kicked a tag we need to move to alternate position,
		// possibly kicking another tag there
		// repeat the process until we succeed or run out of chances
		for (int i = 0; i <= INSERT_ATTEMPTS; i++) {
			curTag = table.swapRandomTagInBucket(curIndex, curTag);
			curIndex = hasher.altIndex(curIndex, curTag);
			if (table.insertToBucket(curIndex, curTag)) {
				count++;
				return true;
			}

		}
		// get here if we couldn't insert and have a random tag floating around

		hasVictim = true;
		victim = new Victim(curIndex, curTag);

		count++;// technically victim is in table
		return true;// technically victim still got put somewhere
	}

	/**
	 * Attempts to insert the victim item if it exists
	 */
	private void insertIfVictim() {
		// trying to insert when filter has victim can create a second
		// victim!
		if (hasVictim && (table.insertToBucket(victim.i1, victim.tag) || table.insertToBucket(victim.i2, victim.tag))) {
			hasVictim = false;
		}
	}

	@VisibleForTesting
	/**
	 * Checks if a given tag is in the victim.
	 * 
	 * @param tagToCheck
	 *            the tag to check
	 * @return true if tag is stored in victim
	 */
	// NOTE: NULL CHECK SKIPPED FOR SPEED
	boolean checkIsVictim(@Nullable BucketAndTag tagToCheck) {
		if (hasVictim) {
			if (victim.tag == tagToCheck.tag && (tagToCheck.index == victim.i1 || tagToCheck.index == victim.i2)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Returns {@code true} if the element <i>might</i> have been put in this
	 * Cuckoo filter, {@code false} if this is <i>definitely</i> not the case.
	 * 
	 * @param item
	 *            to check
	 * 
	 * @return true if the item might be in the filter
	 */
	public boolean mightContain(T item) {
		BucketAndTag pos = hasher.generate(item);
		long i1 = pos.index;
		long i2 = hasher.altIndex(pos.index, pos.tag);

		if (table.findTag(i1, i2, pos.tag)) {
			return true;
		}
		return checkIsVictim(pos);
	}

	/**
	 * Deletes an element from this {@code CuckooFilter}. In most cases you
	 * should only delete items that have been previously added to the filter.
	 * Attempting to delete non-existent items may successfully delete the wrong
	 * item in the filter, causing a false negative. False negatives are defined
	 * as( {@code #mightContain(Object)} returning false for an item that
	 * <i>has</i> been added to the filter. Deleting non-existent items doesn't
	 * otherwise adversely affect the state of the filter, so attempting to
	 * delete items that <i>may not</i> have been inserted is fine if false
	 * negatives are acceptable. The false-delete rate is similar to the false
	 * positive rate.
	 *
	 * @return {@code true} if the cuckoo filter deleted this item successfully.
	 *         Returns {@code false} if the item was not found.
	 * 
	 * @param item
	 *            the item to delete
	 */

	public boolean delete(T item) {
		BucketAndTag pos = hasher.generate(item);
		long i1 = pos.index;
		long i2 = hasher.altIndex(pos.index, pos.tag);

		if (table.deleteFromBucket(i1, pos.tag) || table.deleteFromBucket(i2, pos.tag)) {
			count--;
			insertIfVictim();// might as well try to insert again
			return true;
		}
		if (checkIsVictim(pos)) {
			hasVictim = false;
			count--;
			return true;
		}
		return false;
	}

	@Override
	public boolean equals(@Nullable Object object) {
		if (object == this) {
			return true;
		}
		if (object instanceof CuckooFilter) {
			CuckooFilter<?> that = (CuckooFilter<?>) object;
			if (hasVictim) {
				// only compare victim if set, victim data is sometimes stale
				// since we use bool flag to determine if set or not
				return this.hasher.equals(that.hasher) && this.table.equals(that.table) && this.count == that.count
						&& this.hasVictim == that.hasVictim && victim.equals(that.victim);
			}
			return this.hasher.equals(that.hasher) && this.table.equals(that.table) && this.count == that.count
					&& this.hasVictim == that.hasVictim;
		}
		return false;
	}

	@Override
	public int hashCode() {
		if (hasVictim) {
			return Objects.hash(hasher, table, count, victim);
		}
		return Objects.hash(hasher, table, count);
	}

	/**
	 * Creates a new {@code CuckooFilter} that's a copy of this instance. The
	 * new instance is equal to this instance but shares no mutable state. Note
	 * that further {@code #put(Object)}} operations <i>may</i> cause a copy to
	 * diverge even if the same operations are performed to both copies.
	 * 
	 * @return a copy of the filter
	 */
	public CuckooFilter<T> copy() {
		return new CuckooFilter<>(hasher.copy(), table.copy(), count, hasVictim, victim.copy());
	}

}
