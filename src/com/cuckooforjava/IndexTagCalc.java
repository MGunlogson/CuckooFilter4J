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
import java.util.Objects;

import javax.annotation.Nullable;

import com.cuckooforjava.CuckooFilter.Algorithm;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.primitives.Longs;

class BucketAndTag {

	public final long index;
	public final long tag;

	BucketAndTag(long bucketIndex, long tag) {
		this.index = bucketIndex;
		this.tag = tag;
	}
}

class IndexTagCalc<T> implements Serializable {
	private static final long serialVersionUID = -2052598678199099089L;

	private final SerializableSaltedHasher<T> hasher;
	private final long numBuckets;
	private final int tagBits;
	private final int hashLength;

	@VisibleForTesting
	IndexTagCalc(SerializableSaltedHasher<T> hasher, long numBuckets, int tagBits) {
		checkNotNull(hasher);
		checkArgument((numBuckets & -numBuckets) == numBuckets, "Number of buckets (%s) must be a power of two",
				numBuckets);
		checkArgument(tagBits > 0, "Number of tag bits (%s) must be positive", tagBits);
		// no  matter the hash function we use index and tag are always longs.
		// So, make sure user didn't choose obscenely large fingerprints
		checkArgument(tagBits <= 64, "Number of tag bits (%s) must be <= 64", tagBits);
		checkArgument(numBuckets > 1, "Number of buckets (%s) must be more than 1", numBuckets);
		this.hasher = hasher;
		this.numBuckets = numBuckets;
		this.tagBits = tagBits;
		this.hashLength = hasher.codeBitSize();
		checkArgument(isHashConfigurationIsSupported(numBuckets, tagBits, hashLength),
				"Unsupported Hash Configuration! Hash must be 32, 64, or more than 128 bits and index and tag must fit within hash size. Make table smaller, or use a longer hash.");
	}

	public static <T> IndexTagCalc<T> create(Algorithm hasherAlg, Funnel<? super T> funnel, long numBuckets,
			int tagBits) {
		SerializableSaltedHasher<T> hasher = SerializableSaltedHasher.create(hasherAlg, funnel);
		return new IndexTagCalc<>(hasher, numBuckets, tagBits);
	}

	public static <T> IndexTagCalc<T> create(Funnel<? super T> funnel, long numBuckets, int tagBits) {
		int hashBitsNeeded = getTotalBitsNeeded(numBuckets, tagBits);
		return new IndexTagCalc<>(SerializableSaltedHasher.create(hashBitsNeeded, funnel), numBuckets, tagBits);
	}

	public long getNumBuckets() {
		return numBuckets;
	}

	private static int getTotalBitsNeeded(long numBuckets, int tagBits) {
		return getIndexBitsUsed(numBuckets) + tagBits;
	}

	private static int getIndexBitsUsed(long numBuckets) {
		// how many bits of randomness do we need to create a bucketIndex?
		return 64 - Long.numberOfLeadingZeros(numBuckets);
	}

	private static boolean isHashConfigurationIsSupported(long numBuckets, int tagBits, int hashSize) {
		int hashBitsNeeded = getTotalBitsNeeded(numBuckets, tagBits);
		switch (hashSize) {
		case 32:
		case 64:
			return hashBitsNeeded <= hashSize;
		default:
		}
		if(hashSize>=128)
			return tagBits <= 64 && getIndexBitsUsed(numBuckets)<=64;
		return false;
	}

	public BucketAndTag generate(T item) {
		/*
		 * How do we get tag and bucketIndex from a single 32 bit hash? Max
		 * filter size is constrained to 32 bits of bits (by BitSet) So, the bit
		 * offset for any bit cannot exceed 32 bit boundary. Since max bit
		 * offset is BUCKET_SIZE*bucketIndex*tagBits, we can never use more than
		 * 32 bits of hash for tagBits+bucketIndex
		 */
		long tag=0;
		long bucketIndex=0;
		HashCode code = hasher.hashObj(item);
		// 32 bit hash
		if (hashLength == 32) {
			int hashVal = code.asInt();
			bucketIndex = getBucketIndex32(hashVal);
			// loop until tag isn't equal to empty bucket (0)
			tag = getTagValue32(hashVal);
			for (int salt = 1; tag == 0; salt++) {
				hashVal = hasher.hashObjWithSalt(item, salt).asInt();
				tag = getTagValue32(hashVal);
				assert salt < 100;// shouldn't happen in our timeline
			}
		} else if (hashLength == 64) {
			long hashVal = code.asLong();
			bucketIndex = getBucketIndex64(hashVal);
			// loop until tag isn't equal to empty bucket (0)
			tag = getTagValue64(hashVal);
			for (int salt = 1; tag == 0; salt++) {
				hashVal = hasher.hashObjWithSalt(item, salt).asLong();
				tag = getTagValue64(hashVal);
				assert salt < 100;// shouldn't happen in our timeline
			}
		}
		//>=128
		else
		{
			byte[] hashVal = code.asBytes();
			bucketIndex = getBucketIndex64(longFromLowBytes(hashVal));
			// loop until tag isn't equal to empty bucket (0)
			tag = getTagValue64(longFromHighBytes(hashVal));
			for (int salt = 1; tag == 0; salt++) {
				hashVal = hasher.hashObjWithSalt(item, salt).asBytes();
				tag = getTagValue64(longFromHighBytes(hashVal));
				assert salt < 100;// shouldn't happen in our timeline
			}
		}
		return new BucketAndTag(bucketIndex, tag);
	}

	@VisibleForTesting
	long getTagValue32(int hashVal) {
		/*
		 * for the tag we take the bits from the right of the hash. Since tag
		 * isn't a number we just zero the bits we aren't using. We technically
		 * DONT need to do this(we can just ignore the bits we don't want), but
		 * it makes testing easier
		 */
		// shift out bits we don't need, then shift back to right side
		int unusedBits = Integer.SIZE - tagBits;
		return (hashVal << unusedBits) >>> unusedBits;
	}

	@VisibleForTesting
	long getBucketIndex32(int hashVal) {
		// take index bits from left end of hash
		// just use everything we're not using for tag, why not
		return hashIndex(hashVal >>> tagBits);
	}
	@VisibleForTesting
	long getTagValue64(long hashVal) {
		/*
		 * for the tag we take the bits from the right of the hash. Since tag
		 * isn't a number we just zero the bits we aren't using. We technically
		 * DONT need to do this(we can just ignore the bits we don't want), but
		 * it makes testing easier
		 */
		// shift out bits we don't need, then shift back to right side
		//NOTE: must be long because java will only shift up to 31 bits if right operand is an int!!
		long unusedBits = Long.SIZE - tagBits;
		return (hashVal << unusedBits) >>> unusedBits;
	}

	@VisibleForTesting
	long getBucketIndex64(long hashVal) {
		// take index bits from left end of hash
		// just use everything we're not using for tag, why not
		return hashIndex(hashVal >>> tagBits);
	}
	private long longFromHighBytes(byte[] bytes)
	{
		return Longs.fromBytes(bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]);
	}
	private long longFromLowBytes(byte [] bytes)
	{
		return Longs.fromBytes(bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]);
	}
	public long altIndex(long bucketIndex, long tag) {
		/*
		 * 0xc4ceb9fe1a85ec53L hash mixing constant from MurmurHash3...interesting. Similar value used in
		 * reference implementation https://github.com/efficient/cuckoofilter/
		 */
		long altIndex = bucketIndex ^ (tag * 0xc4ceb9fe1a85ec53L);
		// flip bits if negative
		if (altIndex < 0)
			altIndex = ~altIndex;
		// now pull into valid range
		return hashIndex(altIndex);
	}

	public long hashIndex(long altIndex) {
		/*
		 * we always need to return a bucket index within table range if we try
		 * to range it later during read/write things will go terribly wrong
		 * since the index becomes circular
		 */
		return altIndex % numBuckets;
	}

	@Override
	public boolean equals(@Nullable Object object) {
		if (object == this) {
			return true;
		}
		if (object instanceof IndexTagCalc) {
			IndexTagCalc<?> that = (IndexTagCalc<?>) object;
			return this.hasher.equals(that.hasher) && this.numBuckets == that.numBuckets
					&& this.tagBits == that.tagBits;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(hasher, numBuckets, tagBits);
	}

	public IndexTagCalc<T> copy() {
		return new IndexTagCalc<>(hasher.copy(), numBuckets, tagBits);
	}

}
