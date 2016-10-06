package com.cuckooforjava;

import java.util.concurrent.locks.StampedLock;

import com.google.common.annotations.VisibleForTesting;

public class SegmentedBucketLocker  {
	private final StampedLock[] lockAry;
	private final int concurrentSegments;

	public SegmentedBucketLocker(int concurrentSegments) {
		this.lockAry = new StampedLock[concurrentSegments];
		for(int i=0;i<lockAry.length;i++)
		{
			lockAry[i] = new StampedLock();
		}
		this.concurrentSegments = concurrentSegments;
	}

	@VisibleForTesting
	private int getBucketLock(long bucketIndex) {
		return (int) (bucketIndex % concurrentSegments);
	}

	public void lockBucketsWrite(long i1, long i2) {
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
	public void lockBucketsRead(long i1, long i2) {
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.cuckooforjava.BucketLocker#unlockBucketsWrite(long, long)
	 */
	public void unlockBucketsWrite(long i1, long i2) {
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

	public void unlockBucketsRead(long i1, long i2) {
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

	public void lockAllBucketsRead()
	{
		for(StampedLock lock : lockAry)
		{
			lock.readLock();
		}
	} 
	public void unlockAllBucketsRead()
	{
		for(StampedLock lock : lockAry)
		{
			lock.tryUnlockRead();
		}
	}

	public void lockSingleBucketWrite(long i1) {
		int bucketLockIdx = getBucketLock(i1);
		lockAry[bucketLockIdx].writeLock();
	}


	public void unlockSingleBucketWrite(long i1) {
		int bucketLockIdx = getBucketLock(i1);
		lockAry[bucketLockIdx].tryUnlockWrite();
	}


	public void lockSingleBucketRead(long i1) {
		int bucketLockIdx = getBucketLock(i1);
		lockAry[bucketLockIdx].readLock();
	}


	public void unlockSingleBucketRead(long i1) {
		int bucketLockIdx = getBucketLock(i1);
		lockAry[bucketLockIdx].tryUnlockRead();
	}

}