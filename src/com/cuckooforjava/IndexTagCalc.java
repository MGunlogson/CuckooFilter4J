/*
 *MIT License
 *
 *Copyright (c) 2016 Mark Gunlogson
 *
 *Permission is hereby granted, free of charge, to any person obtaining a copy
 *of this software and associated documentation files (the "Software"), to deal
 *in the Software without restriction, including without limitation the rights
 *to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *copies of the Software, and to permit persons to whom the Software is
 *furnished to do so, subject to the following conditions:
 *
 *The above copyright notice and this permission notice shall be included in all
 *copies or substantial portions of the Software.
 *
 *THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *SOFTWARE.
*/

package com.cuckooforjava;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;

class BucketAndTag {

	public final int index;
	public final int tag;

	BucketAndTag(int bucketIndex, int tag) {
		this.index = bucketIndex;
		this.tag = tag;
	}
}

class IndexTagCalc<T> implements Serializable {
	private static final long serialVersionUID = -2052598678199099089L;

	private final SerializableSaltedHasher<T> hasher;
	private final int numBuckets;
	private final int tagBits;

	public IndexTagCalc(CuckooFilter.Algorithm hasherAlg, Funnel<? super T> funnel, int numBuckets, int tagBits) {
		// instantiated saltedhasher will check its own args :)
		this(new SerializableSaltedHasher<>(hasherAlg, funnel), numBuckets, tagBits);
	}

	@VisibleForTesting
	public IndexTagCalc(SerializableSaltedHasher<T> hasher, int numBuckets, int tagBits) {
		checkNotNull(hasher);
		checkArgument((numBuckets & -numBuckets) == numBuckets, "Number of buckets (%s) must be a multiple of two",
				numBuckets);
		checkArgument(tagBits > 0, "Number of tag bits (%s) must be positive", tagBits);
		checkArgument(tagBits < 28, "Number of tag bits (%s) must be less than 28", tagBits);
		checkArgument(numBuckets > 1, "Number of buckets (%s) must be more than 1", numBuckets);
		int indexHashBitsNotUsed = getIndexTagBitsNotUsed(numBuckets);
		int leftoverBits = indexHashBitsNotUsed - tagBits;
		checkArgument(leftoverBits > 1,
				"Table configuration exhausts 32 bit hash. Make table smaller (less keys or higher fpp). (%s) bits used.",
				leftoverBits);
		this.hasher = hasher;
		this.numBuckets = numBuckets;
		this.tagBits = tagBits;
	}

	int getNumBuckets() {
		return numBuckets;
	}

	int getIndexTagBitsNotUsed(int numBuckets) {
		// how many bits of randomness do we need to create a bucketIndex?
		return Integer.numberOfLeadingZeros(numBuckets);
	}

	public BucketAndTag generate(T item) {
		/*
		 * How do we get tag and bucketIndex from a single 32 bit hash? Max
		 * filter size is constrained to 32 bits of bits (by BitSet) So, the bit
		 * offset for any bit cannot exceed 32 bit boundary. Since max bit
		 * offset is BUCKET_SIZE*bucketIndex*tagBits, we can never use more than
		 * 32 bits of hash for tagBits+bucketIndex
		 */
		int tag;
		int bucketIndex;
		HashCode code = hasher.hashObj(item);
		int hashVal = code.asInt();
		bucketIndex = getBucketIndex(hashVal);
		// loop until tag isn't equal to empty bucket (0)
		tag = getTagValue(hashVal);
		for (int salt = 1; tag == 0; salt++) {
			hashVal = hasher.hashObjWithSalt(item, salt).asInt();
			tag = getTagValue(hashVal);
			assert salt < 100;// shouldn't happen in our timeline
		}
		return new BucketAndTag(bucketIndex, tag);
	}

	@VisibleForTesting
	int getTagValue(int hashVal) {
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
	int getBucketIndex(int hashVal) {
		// take index bits from left end of hash
		//just use everything we're not using for tag, why not
		return hashIndex(hashVal >>> tagBits);
	}

	public int altIndex(int bucketIndex, int tag) {
		/* 0x5bd1e995 hash constant from MurmurHash2...interesting. also used in
		 * reference implementation https://github.com/efficient/cuckoofilter/ 
		 */
		int altIndex= bucketIndex ^ (tag * 0x5bd1e995);
		//flip bits if negative
		if(altIndex<0)
			altIndex = ~altIndex;
		//now pull into valid range
		return hashIndex(altIndex);
	}
	
	public int hashIndex(int index)
	{
		/*we always need to return a bucket index within table range
		 * if we try to range it later during read/write 
		 * things will go terribly wrong since the index becomes circular */
		return index % numBuckets;
	}

	@Override
	public boolean equals(@Nullable Object object) {
		if (object == this) {
			return true;
		}
		if (object instanceof IndexTagCalc) {
			IndexTagCalc<?> that = (IndexTagCalc<?>) object;
			return this.hasher.equals(that.hasher) && this.numBuckets == that.numBuckets && this.tagBits == that.tagBits;
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
