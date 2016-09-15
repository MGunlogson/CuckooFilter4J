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

package cuckooforjava;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;

import cuckooforjava.SerializableSaltedHasher.Algorithm;

class BucketAndTag {

	public final int bucketIndex;
	public final int tag;

	BucketAndTag(int bucketIndex, int tag) {
		this.bucketIndex = bucketIndex;
		this.tag = tag;
	}
}

class IndexTagCalc<T> implements Serializable {
	/*
	 * Why is it so important that numBuckets is a power of 2? Look up modulo
	 * bias. Essentially, taking a remainder to fit a number into a range will
	 * always produce a bias towards certain numbers depending on your divisor,
	 * with one exception. There is no bias produced when using modulo to range
	 * a large int into a smaller range when the divisor is a power of 2.
	 */
	private static final long serialVersionUID = -2052598678199099089L;

	private final SerializableSaltedHasher<T> hasher;
	private final int numBuckets;
	private final int tagBits;
	private final int indexHashBitsNotUsed;

	public IndexTagCalc(Algorithm hasherAlg, Funnel<? super T> funnel, int numBuckets, int tagBits) {
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
		this.indexHashBitsNotUsed = getIndexTagBitsNotUsed(numBuckets);
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
		assert bucketIndex >= 0;
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
		int highBits = hashVal >>> indexHashBitsNotUsed;
		// since we shifted sign bit away, number will always be positive
		// we can mod against numbuckets to pull index into proper range since
		// numBuckets is power of 2
		return highBits % numBuckets;
	}

	public int alternateIndex(int bucketIndex, int tag) {
		/*
		 * 0x5bd1e995 hash constant from MurmurHash2...interesting. also used in
		 * c implementation https://github.com/efficient/cuckoofilter/ TODO:
		 * maybe we should just run the hash algorithm again?
		 */
		int reHashed = bucketIndex ^ (tag * 0x5bd1e995);
		// flip bits if negative,force positive bucket index
		if (reHashed < 0)
			reHashed = ~reHashed;
		// we essentially have a random 31 bit positive # at this point
		// since numBuckets is a power of 2 we can still mod into correct range
		return reHashed % numBuckets;
	}

	@Override
	public boolean equals(@Nullable Object object) {
		if (object == this) {
			return true;
		}
		if (object instanceof IndexTagCalc) {
			IndexTagCalc<?> that = (IndexTagCalc<?>) object;
			return this.hasher.equals(that.hasher) && this.numBuckets == that.numBuckets && this.tagBits == that.tagBits
					&& this.indexHashBitsNotUsed == that.indexHashBitsNotUsed;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(hasher, numBuckets, tagBits, indexHashBitsNotUsed);
	}

	public IndexTagCalc<T> copy() {
		return new IndexTagCalc<T>(hasher.copy(), numBuckets, tagBits);
	}

}
