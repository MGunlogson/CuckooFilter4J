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

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.lucene.util.LongBitSet;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.math.IntMath;
import com.google.common.math.LongMath;

class FilterTable implements Serializable {
	private static final long serialVersionUID = 4172048932165857538L;
	/*
	 * NOTE: Google's Guava library uses a custom BitSet implementation that
	 * looks to be adapted from the Lucene project. Guava project notes show
	 * this seems to be done for faster serialization and support for
	 * longs(giant filters)
	 * 
	 * NOTE: for speed, we don't check for inserts into invalid bucket indexes
	 * or bucket positions!
	 */
	private final LongBitSet memBlock;

	private final int bitsPerTag;

	private final long maxKeys;
	private final long numBuckets;

	private FilterTable(LongBitSet memBlock, int bitsPerTag, long maxKeys, long numBuckets) {
		this.bitsPerTag = bitsPerTag;
		this.memBlock = memBlock;
		this.maxKeys = maxKeys;
		this.numBuckets = numBuckets;
	}

	public static FilterTable create(int bitsPerTag, long numBuckets, long maxKeys) {
		// why would this ever happen?
		checkArgument(bitsPerTag < 48, "tagBits (%s) should be less than 48 bits", bitsPerTag);
		// shorter fingerprints don't give us a good fill capacity
		checkArgument(bitsPerTag > 4, "tagBits (%s) must be > 4", bitsPerTag);
		checkArgument(numBuckets > 1, "numBuckets (%s) must be > 1", numBuckets);
		checkArgument(maxKeys > 1, "maxKeys (%s) must be > 1", maxKeys);
		// checked so our implementors don't get too.... "enthusiastic" with
		// table size
		long bitsPerBucket = IntMath.checkedMultiply(CuckooFilter.BUCKET_SIZE, bitsPerTag);
		long bitSetSize = LongMath.checkedMultiply(bitsPerBucket, numBuckets);
		LongBitSet memBlock = new LongBitSet(bitSetSize);
		return new FilterTable(memBlock, bitsPerTag, maxKeys, numBuckets);
	}

	public boolean insertToBucket(long bucketIndex, long tag) {

		for (int i = 0; i < CuckooFilter.BUCKET_SIZE; i++) {
			if (checkTag(bucketIndex, i, 0)) {
				writeTagNoClear(bucketIndex, i, tag);
				return true;
			}
		}
		return false;
	}

	public long swapRandomTagInBucket(long curIndex, long tag) {
		int randomBucketPosition = ThreadLocalRandom.current().nextInt(CuckooFilter.BUCKET_SIZE);
		return readTagAndSet(curIndex, randomBucketPosition, tag);
	}

	public boolean findTag(long i1, long i2, long tag) {
		for (int i = 0; i < CuckooFilter.BUCKET_SIZE; i++) {
			if (checkTag(i1, i, tag) || checkTag(i2, i, tag))
				return true;
		}
		return false;
	}

	public long getStorageSize() {
		// NOTE: checked source in current Lucene LongBitSet class for thread
		// safety, make sure it stays this way if you update the class.
		return memBlock.length();
	}

	public boolean deleteFromBucket(long i1, long tag) {
		for (int i = 0; i < CuckooFilter.BUCKET_SIZE; i++) {
			if (checkTag(i1, i, tag)) {
				deleteTag(i1, i);
				return true;
			}
		}
		return false;
	}

	@VisibleForTesting
	long readTag(long bucketIndex, int posInBucket) {
		long tagStartIdx = getTagOffset(bucketIndex, posInBucket);
		long tag = 0;
		long tagEndIdx = tagStartIdx + bitsPerTag;
		// looping over true bits per nextBitSet javadocs
		for (long i = memBlock.nextSetBit(tagStartIdx); i >= 0 && i < tagEndIdx; i = memBlock.nextSetBit(i + 1L)) {
			// set corresponding bit in tag
			tag |= 1 << (i - tagStartIdx);
		}
		return tag;
	}

	/**
	 * reads and sets bits at same time for max speedification
	 */
	@VisibleForTesting
	long readTagAndSet(long bucketIndex, int posInBucket, long newTag) {
		long tagStartIdx = getTagOffset(bucketIndex, posInBucket);
		long tag = 0;
		long tagEndIdx = tagStartIdx + bitsPerTag;
		int tagPos = 0;
		for (long i = tagStartIdx; i < tagEndIdx; i++) {
			if ((newTag & (1L << tagPos)) != 0) {
				if (memBlock.getAndSet(i)) {
					tag |= 1 << tagPos;
				}
			} else {
				if (memBlock.getAndClear(i)) {
					tag |= 1 << tagPos;
				}
			}
			tagPos++;
		}
		return tag;
	}

	/**
	 * Faster than regular read because it stops checking if it finds a
	 * non-matching bit.
	 */
	@VisibleForTesting
	boolean checkTag(long bucketIndex, int posInBucket, long tag) {
		long tagStartIdx = getTagOffset(bucketIndex, posInBucket);
		long tagEndIdx = tagStartIdx + bitsPerTag;
		int tagPos = 0;
		for (long i = tagStartIdx; i < tagEndIdx; i++) {
			boolean tagBitIsSet = (tag & (1L << tagPos)) != 0;
			if (memBlock.get(i) != tagBitIsSet)
				return false;
			tagPos++;
		}
		return true;
	}

	/**
	 * Similar to checkTag() except it counts the number of matches in the
	 * bucket.
	 */
	@VisibleForTesting
	int countTag(long i1, long i2, long tag) {
		int tagCount = 0;
		for (int posInBucket = 0; posInBucket < CuckooFilter.BUCKET_SIZE; posInBucket++) {
			if (checkTag(i1, posInBucket, tag))
				tagCount++;
			if (checkTag(i2, posInBucket, tag))
				tagCount++;
		}
		return tagCount;
	}

	/**
	 * faster than regular write because it assumes tag starts with all zeros
	 */
	@VisibleForTesting
	void writeTagNoClear(long bucketIndex, int posInBucket, long tag) {
		long tagStartIdx = getTagOffset(bucketIndex, posInBucket);
		// BIT BANGIN YEAAAARRHHHGGGHHH
		for (int i = 0; i < bitsPerTag; i++) {
			// second arg just does bit test in tag
			if ((tag & (1L << i)) != 0) {
				memBlock.set(tagStartIdx + i);
			}
		}
	}

	// not used
	// @VisibleForTesting
	// void writeTagWithClear(long bucketIndex, int posInBucket, long tag) {
	// long tagStartIdx = getTagOffset(bucketIndex, posInBucket);
	// // BIT BANGIN YEAAAARRHHHGGGHHH
	// for (int i = 0; i < bitsPerTag; i++) {
	// // second arg just does bit test in tag
	// if ((tag & (1L << i)) != 0) {
	// memBlock.set(tagStartIdx + i);
	// } else {
	// memBlock.clear(tagStartIdx + i);
	// }
	// }
	// }

	@VisibleForTesting
	void deleteTag(long bucketIndex, int posInBucket) {
		long tagStartIdx = getTagOffset(bucketIndex, posInBucket);
		memBlock.clear(tagStartIdx, tagStartIdx + bitsPerTag);
	}

	private long getTagOffset(long bucketIndex, int posInBucket) {
		return (bucketIndex * CuckooFilter.BUCKET_SIZE * bitsPerTag) + (posInBucket * bitsPerTag);
	}

	@Override
	public boolean equals(@Nullable Object object) {
		if (object == this) {
			return true;
		}
		if (object instanceof FilterTable) {
			FilterTable that = (FilterTable) object;
			return this.bitsPerTag == that.bitsPerTag && this.memBlock.equals(that.memBlock)
					&& this.maxKeys == that.maxKeys && this.numBuckets == that.numBuckets;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(bitsPerTag, memBlock, maxKeys, numBuckets);
	}

	public FilterTable copy() {
		return new FilterTable(memBlock.clone(), bitsPerTag, maxKeys, numBuckets);
	}

}
