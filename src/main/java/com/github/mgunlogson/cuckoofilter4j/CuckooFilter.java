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
   limitations under the License.
*/

package com.github.mgunlogson.cuckoofilter4j;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

import javax.annotation.Nullable;

import com.github.mgunlogson.cuckoofilter4j.Utils.Algorithm;
import com.github.mgunlogson.cuckoofilter4j.Utils.Victim;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Funnel;

/**
 * A Cuckoo filter for instances of {@code T}. Cuckoo filters are probabilistic
 * hash tables similar to Bloom filters but with several advantages. Like Bloom
 * filters, a Cuckoo filter can determine if an object is contained within a set
 * at a specified false positive rate with no false negatives. Like Bloom, a
 * Cuckoo filter can determine if an element is probably inserted or definitely
 * is not. In addition, and unlike standard Bloom filters, Cuckoo filters allow
 * deletions and counting. They also use less space than a Bloom filter for
 * similar performance.
 *
 * <p>
 * The false positive rate of the filter is the probability that
 * {@linkplain #mightContain(Object)}} will erroneously return {@code true} for
 * an object that was not added to the filter. Unlike Bloom filters, a Cuckoo
 * filter will fail to insert when it reaches capacity. If an insert fails
 * {@linkplain #put(Object)} will {@code return false} .
 * 
 * <p>
 * Cuckoo filters allow deletion like counting Bloom filters using
 * {@code #delete(Object)}. While counting Bloom filters invariably use more
 * space to allow deletions, Cuckoo filters achieve this with <i>no</i> space or
 * time cost. Like counting variations of Bloom filters, Cuckoo filters have a
 * limit to the number of times you can insert duplicate items. This limit is
 * 8-9 in the current design, depending on internal state. You should never
 * exceed 7 if possible. <i>Reaching this limit can cause further inserts to
 * fail and degrades the performance of the filter</i>. Occasional duplicates
 * will not degrade the performance of the filter but will slightly reduce
 * capacity.
 * 
 * <p>
 * This Cuckoo filter implementation also allows counting the number of inserts
 * for each item using {@code #approximateCount(Object)}. This is probabilistic
 * like the rest of the filter and any error is always an increase. The count
 * will never return less than the number of actual inserts, but may return
 * more. The insert limit of 7 still stands when counting so this is only useful
 * for small numbers.
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
 * functions include SHA and SipHash. All hashes, including non-secure, are
 * internally seeded and salted. Practical attacks against any of them are
 * unlikely.
 * 
 * <p>
 * This implementation of a Cuckoo filter is serializable.
 * 
 * @see <a href="https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf">
 *      paper on Cuckoo filter properties.</a>
 * @see <a href="https://github.com/seiflotfy/cuckoofilter">Golang Cuckoo filter
 *      implementation</a>
 * @see <a href="https://github.com/efficient/cuckoofilter">C++ reference
 *      implementation</a>
 *
 * @param <T>
 *            the type of items that the {@code CuckooFilter} accepts
 * @author Mark Gunlogson
 */
public final class CuckooFilter<T> implements Serializable {

	/*
	 * IMPORTANT THREAD SAFETY NOTES. To prevent deadlocks, all methods needing
	 * multiple locks need to lock the victim first. This is followed by the
	 * segment locks, which need to be locked in ascending order of segment in
	 * the backing lock array. The bucketlocker will always lock multiple
	 * buckets in the same order if you use it properly.
	 * 
	 */
	private static final long serialVersionUID = -1337735144654851942L;
	static final int INSERT_ATTEMPTS = 500;
	static final int BUCKET_SIZE = 4;
	// make sure to update getNeededBitsForFpRate() if changing this... then
	// again don't change this
	private static final double LOAD_FACTOR = 0.955;
	private static final double DEFAULT_FP = 0.01;
	private static final int DEFAULT_CONCURRENCY = 16;

	@VisibleForTesting
	final FilterTable table;
	@VisibleForTesting
	final IndexTagCalc<T> hasher;
	private final AtomicLong count;
	/**
	 * Only stored for serialization since the bucket locker is transient.
	 * equals() and hashcode() just check the concurrency value in the bucket
	 * locker and ignore this
	 */
	private final int expectedConcurrency;
	private final StampedLock victimLock;
	private transient SegmentedBucketLocker bucketLocker;

	@VisibleForTesting
	Victim victim;
	@VisibleForTesting
	boolean hasVictim;

	/**
	 * Creates a Cuckoo filter.
	 */
	private CuckooFilter(IndexTagCalc<T> hasher, FilterTable table, AtomicLong count, boolean hasVictim, Victim victim,
			int expectedConcurrency) {
		this.hasher = hasher;
		this.table = table;
		this.count = count;
		this.hasVictim = hasVictim;
		this.expectedConcurrency = expectedConcurrency;
		// no nulls even if victim hasn't been used!
		if (victim == null)
			this.victim = new Victim();
		else
			this.victim = victim;

		this.victimLock = new StampedLock();
		this.bucketLocker = new SegmentedBucketLocker(expectedConcurrency);
	}

	/***
	 * Builds a Cuckoo Filter. To Create a Cuckoo filter, construct this then
	 * call {@code #build()}.
	 * 
	 * @author Mark Gunlogson
	 *
	 * @param <T>
	 *            the type of item {@code Funnel will use}
	 */
	public static class Builder<T> {
		// required arguments
		private final Funnel<? super T> funnel;
		private final long maxKeys;
		// optional arguments
		private Algorithm hashAlgorithm;
		private double fpp = DEFAULT_FP;
		private int expectedConcurrency = DEFAULT_CONCURRENCY;

		/**
		 * Creates a Builder interface for {@link CuckooFilter CuckooFilter}
		 * with the expected number of insertions using the default false
		 * positive rate, {@code #hashAlgorithm}, and concurrency. The default
		 * false positive rate is 1%. The default hash is Murmur3, automatically
		 * using the 32 bit version for small tables and 128 bit version for
		 * larger ones. The default concurrency is 16 expected threads.
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
		 * It is recommended that the funnel be implemented as a Java enum. This
		 * has the benefit of ensuring proper serialization and deserialization,
		 * which is important since {@link #equals} also relies on object
		 * identity of funnels.
		 *
		 *
		 * @param funnel
		 *            the funnel of T's that the constructed
		 *            {@code CuckooFilter<T>} will use
		 * @param maxKeys
		 *            the number of expected insertions to the constructed
		 *            {@code CuckooFilter<T>}; must be positive
		 * 
		 */
		public Builder(Funnel<? super T> funnel, long maxKeys) {
			checkArgument(maxKeys > 1, "maxKeys (%s) must be > 1, increase maxKeys", maxKeys);
			checkNotNull(funnel);
			this.funnel = funnel;
			this.maxKeys = maxKeys;
		}

		/**
		 * Creates a Builder interface for {@link CuckooFilter CuckooFilter}
		 * with the expected number of insertions using the default false
		 * positive rate, {@code #hashAlgorithm}, and concurrency. The default
		 * false positive rate is 1%. The default hash is Murmur3, automatically
		 * using the 32 bit version for small tables and 128 bit version for
		 * larger ones. The default concurrency is 16 expected threads.
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
		 * It is recommended that the funnel be implemented as a Java enum. This
		 * has the benefit of ensuring proper serialization and deserialization,
		 * which is important since {@link #equals} also relies on object
		 * identity of funnels.
		 *
		 *
		 * @param funnel
		 *            the funnel of T's that the constructed
		 *            {@code CuckooFilter<T>} will use
		 * @param maxKeys
		 *            the number of expected insertions to the constructed
		 *            {@code CuckooFilter<T>}; must be positive
		 * 
		 */
		public Builder(Funnel<? super T> funnel, int maxKeys) {
			this(funnel, (long) maxKeys);
		}

		/**
		 * Sets the false positive rate for the filter. The default is 1%.
		 * Unrealistic values will cause filter creation to fail on
		 * {@code #build()} due to excessively short fingerprints or memory
		 * exhaustion. The filter becomes more space efficient than Bloom
		 * filters below ~0.02 (2%) .
		 * 
		 * @param fpp
		 *            false positive rate ( value is (expected %)/100 ) from 0-1
		 *            exclusive.
		 * @return The builder interface
		 */
		public Builder<T> withFalsePositiveRate(double fpp) {
			checkArgument(fpp > 0, "fpp (%s) must be > 0, increase fpp", fpp);
			checkArgument(fpp < .25, "fpp (%s) must be < 0.25, decrease fpp", fpp);
			this.fpp = fpp;
			return this;
		}

		/**
		 * Sets the hashing algorithm used internally. The default is Murmur3,
		 * 32 or 128 bit sized automatically. Calling this with a Murmur3
		 * variant instead of using the default will disable automatic hash
		 * sizing of Murmur3. The size of the table will be significantly
		 * limited with a 32 bit hash to around 270 MB. Table size is still
		 * limited in certain circumstances when using 64 bit hashes like
		 * SipHash. 128+ bit hashes will allow practically unlimited table size.
		 * In any case, filter creation will fail on {@code #build()} with an
		 * invalid configuration.
		 * 
		 * @param hashAlgorithm the hashing algorithm used by the filter.
		 * @return The builder interface
		 */
		public Builder<T> withHashAlgorithm(Algorithm hashAlgorithm) {
			checkNotNull(hashAlgorithm,
					"hashAlgorithm cannot be null. To use default, build without calling this method.");
			this.hashAlgorithm = hashAlgorithm;
			return this;
		}

		/***
		 * 
		 * Number of simultaneous threads expected to access the filter
		 * concurrently. The default is 16 threads. It is better to overestimate
		 * as the cost of more segments is very small and penalty for contention
		 * is high. This number is not performance critical, any number over the
		 * actual number of threads and within an order of magnitude will work.
		 * <i> THIS NUMBER MUST BE A POWER OF 2</i>
		 * 
		 * @param expectedConcurrency
		 *            expected number of threads accessing the filter
		 *            concurrently.
		 *            
		 * @return The builder interface   
		 *            
		 */
		public Builder<T> withExpectedConcurrency(int expectedConcurrency) {
			checkArgument(expectedConcurrency > 0, "expectedConcurrency (%s) must be > 0.", expectedConcurrency);
			checkArgument((expectedConcurrency & (expectedConcurrency - 1)) == 0,
					"expectedConcurrency (%s) must be a power of two.", expectedConcurrency);
			this.expectedConcurrency = expectedConcurrency;
			return this;
		}

		/**
		 * Builds and returns a {@code CuckooFilter<T>}. Invalid configurations
		 * will fail on this call.
		 * 
		 * @return a Cuckoo filter of type T
		 */
		public CuckooFilter<T> build() {
			int tagBits = Utils.getBitsPerItemForFpRate(fpp, LOAD_FACTOR);
			long numBuckets = Utils.getBucketsNeeded(maxKeys, LOAD_FACTOR, BUCKET_SIZE);
			IndexTagCalc<T> hasher;
			if (hashAlgorithm == null) {
				hasher = IndexTagCalc.create(funnel, numBuckets, tagBits);
			} else
				hasher = IndexTagCalc.create(hashAlgorithm, funnel, numBuckets, tagBits);
			FilterTable filtertbl = FilterTable.create(tagBits, numBuckets);
			return new CuckooFilter<>(hasher, filtertbl, new AtomicLong(0), false, null, expectedConcurrency);
		}
	}


	/**
	 * Gets the current number of items in the Cuckoo filter. Can be higher than
	 * the max number of keys the filter was created to store if it is running
	 * over expected maximum fill capacity. If you need to know the absolute
	 * maximum number of items this filter can contain, call
	 * {@code #getActualCapacity()}. If you just want to check how full the
	 * filter is, it's better to use {@code #getLoadFactor()} than this, which
	 * is bounded at 1.0
	 * 
	 * @return number of items in filter
	 */
	public long getCount() {
		// can return more than maxKeys if running above design limit!
		return count.get();
	}

	/**
	 * Gets the current load factor of the Cuckoo filter. Reasonably sized
	 * filters with randomly distributed values can be expected to reach a load
	 * factor of around 95% (0.95) before insertion failure. Note that during
	 * simultaneous access from multiple threads this may not be exact in rare
	 * cases.
	 * 
	 * @return load fraction of total space used, 0-1 inclusive
	 */
	public double getLoadFactor() {
		return count.get() / (hasher.getNumBuckets() * (double) BUCKET_SIZE);
	}

	/**
	 * Gets the absolute maximum number of items the filter can theoretically
	 * hold. <i>This is NOT the maximum you can expect it to reliably hold.</i>
	 * This should only be used if you understand the source. Internal
	 * restrictions on backing array size and compensation for the expected
	 * filter occupancy on first insert failure nearly always make the filter
	 * larger than requested on creation. This method returns how big the filter
	 * actually is (in items) <i>DO NOT EXPECT IT TO BE ABLE TO HOLD THIS MANY
	 * </i>
	 * 
	 * @return number of keys filter can theoretically hold at 100% fill
	 */
	public long getActualCapacity() {
		return hasher.getNumBuckets() * BUCKET_SIZE;
	}

	/**
	 * Gets the size of the underlying {@code LongBitSet} table for the filter,
	 * in bits. This should only be used if you understand the source.
	 * 
	 * @return space used by table in bits
	 */
	public long getStorageSize() {
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
		bucketLocker.lockBucketsWrite(curIndex, altIndex);
		try {
			if (table.insertToBucket(curIndex, curTag) || table.insertToBucket(altIndex, curTag)) {
				count.incrementAndGet();
				return true;
			}
		} finally {
			bucketLocker.unlockBucketsWrite(curIndex, altIndex);
		}
		// don't do insertion loop if victim slot is already filled
		long victimLockStamp = writeLockVictimIfClear();
		if (victimLockStamp == 0L)
			// victim was set...can't insert
			return false;
		try {
			// fill victim slot and run fun insert method below
			victim.setTag(curTag);
			victim.setI1(curIndex);
			victim.setI2(altIndex);
			hasVictim = true;
			for (int i = 0; i <= INSERT_ATTEMPTS; i++) {
				if (trySwapVictimIntoEmptySpot())
					break;
			}
			/*
			 * count is incremented here because we should never increase count
			 * when not locking buckets or victim. Reason is because otherwise
			 * count may be inconsistent across threads when doing operations
			 * that lock the whole table like hashcode() or equals()
			 */
			count.getAndIncrement();
		} finally {
			victimLock.unlock(victimLockStamp);
		}
		// if we get here, we either managed to insert victim using retries or
		// it's in victim slot from another thread. Either way, it's in the
		// table.
		return true;
	}

	/**
	 * if we kicked a tag we need to move it to alternate position, possibly
	 * kicking another tag there, repeating the process until we succeed or run
	 * out of chances
	 * 
	 * The basic flow below is to insert our current tag into a position in an
	 * already full bucket, then move the tag that we overwrote to it's
	 * alternate index. We repeat this until we move a tag into a non-full
	 * bucket or run out of attempts. This tag shuffling process is what gives
	 * the Cuckoo filter such a high load factor. When we run out of attempts,
	 * we leave the orphaned tag in the victim slot.
	 * 
	 * We need to be extremely careful here to avoid deadlocks and thread stalls
	 * during this process. The most nefarious deadlock is that two or more
	 * threads run out of tries simultaneously and all need a place to store a
	 * victim even though we only have one slot
	 * 
	 */
	private boolean trySwapVictimIntoEmptySpot() {

		long curIndex = victim.getI2();
		// lock bucket. We always use I2 since victim tag is from bucket I1
		bucketLocker.lockSingleBucketWrite(curIndex);
		long curTag = table.swapRandomTagInBucket(curIndex, victim.getTag());
		bucketLocker.unlockSingleBucketWrite(curIndex);
		// new victim's I2 is different as long as tag isn't the same
		long altIndex = hasher.altIndex(curIndex, curTag);
		// try to insert the new victim tag in it's alternate bucket
		bucketLocker.lockSingleBucketWrite(altIndex);
		try {
			if (table.insertToBucket(altIndex, curTag)) {
				hasVictim = false;
				return true;
			} else {
				// still have a victim, but a different one...
				victim.setTag(curTag);
				// new victim always shares I1 with previous victims' I2
				victim.setI1(curIndex);
				victim.setI2(altIndex);
			}
		} finally {
			bucketLocker.unlockSingleBucketWrite(altIndex);
		}
		return false;

	}

	/**
	 * Attempts to insert the victim item if it exists. Remember that inserting
	 * from the victim cache to the main table DOES NOT affect the count since
	 * items in the victim cache are technically still in the table
	 * 
	 */
	private void insertIfVictim() {
		long victimLockstamp = writeLockVictimIfSet();
		if (victimLockstamp == 0L)
			return;
		try {

			// when we get here we definitely have a victim and a write lock
			bucketLocker.lockBucketsWrite(victim.getI1(), victim.getI2());
			try {
				if (table.insertToBucket(victim.getI1(), victim.getTag())
						|| table.insertToBucket(victim.getI2(), victim.getTag())) {
					// set this here because we already have lock
					hasVictim = false;
				}
			} finally {
				bucketLocker.unlockBucketsWrite(victim.getI1(), victim.getI2());
			}
		} finally {
			victimLock.unlock(victimLockstamp);
		}

	}

	/***
	 * Checks if the victim is set using a read lock and upgrades to a write
	 * lock if it is. Will either return a write lock stamp if victim is set, or
	 * zero if no victim.
	 * 
	 * @return a write lock stamp for the Victim or 0 if no victim
	 */
	private long writeLockVictimIfSet() {
		long victimLockstamp = victimLock.readLock();
		if (hasVictim) {
			// try to upgrade our read lock to write exclusive if victim
			long writeLockStamp = victimLock.tryConvertToWriteLock(victimLockstamp);
			// could not get write lock
			if (writeLockStamp == 0L) {
				// so unlock the victim
				victimLock.unlock(victimLockstamp);
				// now just block until we have exclusive lock
				victimLockstamp = victimLock.writeLock();
				// make sure victim is still set with our new write lock
				if (!hasVictim) {
					// victim has been cleared by another thread... so just give
					// up our lock
					victimLock.tryUnlockWrite();
					return 0L;
				} else
					return victimLockstamp;
			} else {
				return writeLockStamp;
			}
		} else {
			victimLock.unlock(victimLockstamp);
			return 0L;
		}
	}

	/***
	 * Checks if the victim is clear using a read lock and upgrades to a write
	 * lock if it is clear. Will either return a write lock stamp if victim is
	 * clear, or zero if a victim is already set.
	 * 
	 * @return a write lock stamp for the Victim or 0 if victim is set
	 */
	private long writeLockVictimIfClear() {
		long victimLockstamp = victimLock.readLock();
		if (!hasVictim) {
			// try to upgrade our read lock to write exclusive if victim
			long writeLockStamp = victimLock.tryConvertToWriteLock(victimLockstamp);
			// could not get write lock
			if (writeLockStamp == 0L) {
				// so unlock the victim
				victimLock.unlock(victimLockstamp);
				// now just block until we have exclusive lock
				victimLockstamp = victimLock.writeLock();
				// make sure victim is still clear with our new write lock
				if (!hasVictim)
					return victimLockstamp;
				else {
					// victim has been set by another thread... so just give up
					// our lock
					victimLock.tryUnlockWrite();
					return 0L;
				}
			} else {
				return writeLockStamp;
			}
		} else {
			victimLock.unlock(victimLockstamp);
			return 0L;
		}
	}

	@VisibleForTesting
	/**
	 * Checks if a given tag is the victim.
	 * 
	 * @param tagToCheck
	 *            the tag to check
	 * @return true if tag is stored in victim
	 */
	boolean checkIsVictim(BucketAndTag tagToCheck) {
		checkNotNull(tagToCheck);
		victimLock.readLock();
		try {
			if (hasVictim) {
				if (victim.getTag() == tagToCheck.tag
						&& (tagToCheck.index == victim.getI1() || tagToCheck.index == victim.getI2())) {
					return true;
				}
			}
			return false;
		} finally {
			victimLock.tryUnlockRead();
		}
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
		bucketLocker.lockBucketsRead(i1, i2);
		try {
			if (table.findTag(i1, i2, pos.tag)) {
				return true;
			}
		} finally {
			bucketLocker.unlockBucketsRead(i1, i2);
		}
		return checkIsVictim(pos);
	}

	/**
	 * This method returns the approximate number of times an item was added to
	 * the filter. This count is probabilistic like the rest of the filter, so
	 * it may occasionally over-count. Since the filter has no false negatives,
	 * <i>the approximate count will always be equal or greater than the actual
	 * count(unless you've been deleting non-existent items)</i>. That is, this
	 * method may return a higher count than the true value, but never lower.
	 * The false inflation chance of the count depends on the filter's false
	 * positive rate, but is generally low for sane configurations.
	 * <p>
	 * NOTE: Inserting the same key more than 7 times will cause a bucket
	 * overflow, greatly decreasing the performance of the filter and making
	 * early insertion failure (less than design load factor) very likely. For
	 * this reason the filter should only be used to count small values.
	 * 
	 * <p>
	 * Also note that getting the count is generally about half as fast as
	 * checking if a filter contains an item.
	 * 
	 * @param item
	 *            item to check
	 * @return Returns a positive integer representing the number of times an
	 *         item was probably added to the filter. Returns zero if the item
	 *         is not in the filter, behaving exactly like
	 *         {@code #mightContain(Object)} in this case.
	 */
	public int approximateCount(T item) {
		BucketAndTag pos = hasher.generate(item);
		long i1 = pos.index;
		long i2 = hasher.altIndex(pos.index, pos.tag);
		int tagCount = 0;
		bucketLocker.lockBucketsRead(i1, i2);
		try {
			tagCount = table.countTag(i1, i2, pos.tag);
		} finally {
			bucketLocker.unlockBucketsRead(i1, i2);
		}
		if (checkIsVictim(pos))
			tagCount++;
		return tagCount;
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
	 * positive rate. False deletes can also cause the
	 * {@code #approximateCount(Object)} to return both lower and higher than
	 * the real count
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
		bucketLocker.lockBucketsWrite(i1, i2);
		boolean deleteSuccess = false;
		try {
			if (table.deleteFromBucket(i1, pos.tag) || table.deleteFromBucket(i2, pos.tag))
				deleteSuccess = true;
		} finally {
			bucketLocker.unlockBucketsWrite(i1, i2);
		}
		// try to insert the victim again if we were able to delete an item
		if (deleteSuccess) {
			count.decrementAndGet();
			insertIfVictim();// might as well try to insert again
			return true;
		}
		// if delete failed but we have a victim, check if the item we're trying
		// to delete IS actually the victim
		long victimLockStamp = writeLockVictimIfSet();
		if (victimLockStamp == 0L)
			return false;
		else {
			try {
				// check victim match
				if (victim.getTag() == pos.tag && (victim.getI1() == pos.index || victim.getI2() == pos.index)) {
					hasVictim = false;
					count.decrementAndGet();
					return true;
				} else
					return false;
			} finally {
				victimLock.unlock(victimLockStamp);
			}
		}
	}

	private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
		// default deserialization
		ois.defaultReadObject();
		// not serializable so we rebuild here
		bucketLocker = new SegmentedBucketLocker(expectedConcurrency);
	}

	@Override
	public boolean equals(@Nullable Object object) {
		if (object == this) {
			return true;
		}
		if (object instanceof CuckooFilter) {
			CuckooFilter<?> that = (CuckooFilter<?>) object;
			victimLock.readLock();
			bucketLocker.lockAllBucketsRead();
			try {
				if (hasVictim) {
					// only compare victim if set, victim is sometimes stale
					// since we use bool flag to determine if set or not
					return this.hasher.equals(that.hasher) && this.table.equals(that.table)
							&& this.count.get() == that.count.get() && this.hasVictim == that.hasVictim
							&& victim.equals(that.victim);
				}
				return this.hasher.equals(that.hasher) && this.table.equals(that.table)
						&& this.count.get() == that.count.get() && this.hasVictim == that.hasVictim;
			} finally {
				bucketLocker.unlockAllBucketsRead();
				victimLock.tryUnlockRead();
			}
		}
		return false;
	}

	@Override
	public int hashCode() {
		victimLock.readLock();
		bucketLocker.lockAllBucketsRead();
		try {
			if (hasVictim) {
				return Objects.hash(hasher, table, count.get(), victim);
			}
			return Objects.hash(hasher, table, count.get());
		} finally {
			bucketLocker.unlockAllBucketsRead();
			victimLock.tryUnlockRead();
		}
	}

	/**
	 * Creates a new {@code CuckooFilter} that's a copy of this instance. The
	 * new instance is equal to this instance but shares no mutable state. Note
	 * that further {@code #put(Object)}} operations <i>may</i> cause a copy to
	 * diverge even if the same operations are performed to both filters since
	 * bucket swaps are essentially random.
	 * 
	 * @return a copy of the filter
	 */
	public CuckooFilter<T> copy() {
		victimLock.readLock();
		bucketLocker.lockAllBucketsRead();
		try {
			return new CuckooFilter<>(hasher.copy(), table.copy(), count, hasVictim, victim.copy(),
					expectedConcurrency);
		} finally {
			bucketLocker.unlockAllBucketsRead();
			victimLock.tryUnlockRead();
		}
	}

}
