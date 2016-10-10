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

import java.util.concurrent.locks.StampedLock;

import com.google.common.annotations.VisibleForTesting;

/**
 * Maintains a lock array corresponding to bucket indexes and the segment of the
 * bitset they belong to. The Cuckoo filter's memory table is split by bucket
 * index into several segments which can be locked for reading/writing
 * individually for thread safety. This class holds the locks and contains
 * helper methods for unlocking and unlocking and avoiding deadlocks
 * 
 * @author Mark Gunlogson
 *
 */
final class SegmentedBucketLocker {
	private final StampedLock[] lockAry;
	// must be a power of 2 so no modulo bias
	private final int concurrentSegments;

	SegmentedBucketLocker(int expectedConcurrency) {
		checkArgument(expectedConcurrency > 0, "expectedConcurrency (%s) must be > 0.", expectedConcurrency);
		checkArgument((expectedConcurrency & (expectedConcurrency - 1)) == 0,
				"expectedConcurrency (%s) must be a power of two.", expectedConcurrency);
		// most operations lock two buckets, so for X threads we should have
		// roughly 2X segments.
		this.concurrentSegments = expectedConcurrency * 2;
		this.lockAry = new StampedLock[concurrentSegments];
		for (int i = 0; i < lockAry.length; i++) {
			lockAry[i] = new StampedLock();
		}

	}

	/**
	 *   returns the segment that bucket index belongs to
	 */
	@VisibleForTesting
	private int getBucketLock(long bucketIndex) {
		return (int) (bucketIndex % concurrentSegments);
	}
/**
 * Locks segments corresponding to bucket indexes in specific order to prevent deadlocks
 */
	void lockBucketsWrite(long i1, long i2) {
		int bucket1LockIdx = getBucketLock(i1);
		int bucket2LockIdx = getBucketLock(i2);
		// always lock segments in same order to avoid deadlocks
		if (bucket1LockIdx < bucket2LockIdx) {
			lockAry[bucket1LockIdx].writeLock();
			lockAry[bucket2LockIdx].writeLock();
		} else if (bucket1LockIdx > bucket2LockIdx) {
			lockAry[bucket2LockIdx].writeLock();
			lockAry[bucket1LockIdx].writeLock();
		}
		// if we get here both indexes are on same segment so only lock once!!!
		else {
			lockAry[bucket1LockIdx].writeLock();
		}
	}
	/**
	 * Locks segments corresponding to bucket indexes in specific order to prevent deadlocks
	 */
	void lockBucketsRead(long i1, long i2) {
		int bucket1LockIdx = getBucketLock(i1);
		int bucket2LockIdx = getBucketLock(i2);
		// always lock segments in same order to avoid deadlocks
		if (bucket1LockIdx < bucket2LockIdx) {
			lockAry[bucket1LockIdx].readLock();
			lockAry[bucket2LockIdx].readLock();
		} else if (bucket1LockIdx > bucket2LockIdx) {
			lockAry[bucket2LockIdx].readLock();
			lockAry[bucket1LockIdx].readLock();
		}
		// if we get here both indexes are on same segment so only lock once!!!
		else {
			lockAry[bucket1LockIdx].readLock();
		}
	}

	/**
	 * Unlocks segments corresponding to bucket indexes in specific order to prevent deadlocks
	 */
	void unlockBucketsWrite(long i1, long i2) {
		int bucket1LockIdx = getBucketLock(i1);
		int bucket2LockIdx = getBucketLock(i2);
		// always unlock segments in same order to avoid deadlocks
		if (bucket1LockIdx == bucket2LockIdx) {
			lockAry[bucket1LockIdx].tryUnlockWrite();
			return;
		}
		lockAry[bucket1LockIdx].tryUnlockWrite();
		lockAry[bucket2LockIdx].tryUnlockWrite();
	}
	/**
	 * Unlocks segments corresponding to bucket indexes in specific order to prevent deadlocks
	 */
	void unlockBucketsRead(long i1, long i2) {
		int bucket1LockIdx = getBucketLock(i1);
		int bucket2LockIdx = getBucketLock(i2);
		// always unlock segments in same order to avoid deadlocks
		if (bucket1LockIdx == bucket2LockIdx) {
			lockAry[bucket1LockIdx].tryUnlockRead();
			return;
		}
		lockAry[bucket1LockIdx].tryUnlockRead();
		lockAry[bucket2LockIdx].tryUnlockRead();
	}
	/**
	 * Locks all segments in specific order to prevent deadlocks
	 */
	void lockAllBucketsRead() {
		for (StampedLock lock : lockAry) {
			lock.readLock();
		}
	}
	/**
	 * Unlocks all segments
	 */
	void unlockAllBucketsRead() {
		for (StampedLock lock : lockAry) {
			lock.tryUnlockRead();
		}
	}

	void lockSingleBucketWrite(long i1) {
		int bucketLockIdx = getBucketLock(i1);
		lockAry[bucketLockIdx].writeLock();
	}

	void unlockSingleBucketWrite(long i1) {
		int bucketLockIdx = getBucketLock(i1);
		lockAry[bucketLockIdx].tryUnlockWrite();
	}

	void lockSingleBucketRead(long i1) {
		int bucketLockIdx = getBucketLock(i1);
		lockAry[bucketLockIdx].readLock();
	}

	void unlockSingleBucketRead(long i1) {
		int bucketLockIdx = getBucketLock(i1);
		lockAry[bucketLockIdx].tryUnlockRead();
	}

}