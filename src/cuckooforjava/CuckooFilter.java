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
import java.math.RoundingMode;
import java.util.Objects;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Funnel;
import com.google.common.math.DoubleMath;

import cuckooforjava.SerializableSaltedHasher.Algorithm;

public final class CuckooFilter<T> implements Serializable {

	private static final long serialVersionUID = -1337735144654851942L;
	public static final int INSERT_ATTEMPTS = 500;
	public static final int BUCKET_SIZE = 4;
	// make sure to update getNeededBitsForFpRate() if changing this... then
	// again don't change this
	public static final double LOAD_FACTOR = 0.955;
	public static final double DEFAULT_FP = 0.01;

	private FilterTable table;
	private IndexTagCalc<T> hasher;
	private int count;
	// when the filter becomes completely full, the last item that fails to be
	// repositioned will be left without a home
	@VisibleForTesting
	Victim victim;
	@VisibleForTesting
	boolean hasVictim;

	class Victim implements Serializable {
		private static final long serialVersionUID = -984233593241086192L;
		int i1;
		int i2;
		int tag;

		Victim(int bucketIndex, int tag) {
			this.i1 = bucketIndex;
			this.i2 = hasher.altIndex(bucketIndex, tag);
			this.tag = tag;
		}
	}

	private CuckooFilter(IndexTagCalc<T> hasher, FilterTable table, int count) {
		this.hasher = hasher;
		this.table = table;
		this.count = count;
	}

	public static <T> CuckooFilter<T> create(Funnel<? super T> funnel, int maxKeys) {
		return create(funnel, maxKeys, DEFAULT_FP, Algorithm.Murmur3_32);
	}

	public static <T> CuckooFilter<T> create(Funnel<? super T> funnel, int maxKeys, double fpp) {
		return create(funnel, maxKeys, fpp, Algorithm.Murmur3_32);
	}

	public static <T> CuckooFilter<T> create(Funnel<? super T> funnel, int maxKeys, Algorithm hashAlgorithm) {
		return create(funnel, maxKeys, DEFAULT_FP, hashAlgorithm);
	}

	public static <T> CuckooFilter<T> create(Funnel<? super T> funnel, int maxKeys, double fpp,
			Algorithm hashAlgorithm) {
		checkArgument(maxKeys > 1, "maxKeys (%s) must be > 1, increase maxKeys", maxKeys);
		checkArgument(fpp > 0, "fpp (%s) must be > 0, increase fpp", fpp);
		checkArgument(fpp < .25, "fpp (%s) must be < 0.25, decrease fpp", fpp);
		checkNotNull(hashAlgorithm);
		checkNotNull(funnel);
		int tagBits = getBitsPerItemForFpRate(fpp);
		int numBuckets = getBucketsNeeded(maxKeys);
		IndexTagCalc<T> hasher = new IndexTagCalc<>(hashAlgorithm, funnel, numBuckets, tagBits);
		FilterTable filtertbl = FilterTable.create(tagBits, numBuckets, maxKeys);
		return new CuckooFilter<>(hasher, filtertbl, 0);

	}

	private static int getBitsPerItemForFpRate(double fpProb) {
		/*
		 * equation from Cuckoo Filter: Practically Better Than Bloom Bin Fan,
		 * David G. Andersen, Michael Kaminsky , Michael D. Mitzenmacher
		 */
		return DoubleMath.roundToInt(DoubleMath.log2((1 / fpProb) + 3) / LOAD_FACTOR, RoundingMode.UP);
	}

	private static int getBucketsNeeded(int maxKeys) {
		/*
		 * force a power-of-two bucket count so hash functions for bucket index
		 * can hashBits%numBuckets and get randomly distributed index
		 */
		int bucketsNeeded = DoubleMath.roundToInt((1.0 / LOAD_FACTOR) * maxKeys / BUCKET_SIZE, RoundingMode.UP);
		// get next biggest power of 2
		int bitPos = Integer.highestOneBit(bucketsNeeded);
		if (bucketsNeeded > bitPos)
			bitPos = bitPos << 1;
		return bitPos;
	}

	public int getCount() {
		// can return more than maxKeys if running above design limit!
		return count;
	}

	public double getLoadFactor() {
		return count / (hasher.getNumBuckets() * (double) BUCKET_SIZE);
	}

	public int getStorageSize() {
		return table.getStorageSize();
	}

	public boolean put(T item) {
		BucketAndTag pos = hasher.generate(item);
		int curTag = pos.tag;
		int curIndex = pos.index;
		int altIndex = hasher.altIndex(curIndex, curTag);
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

	private void insertIfVictim() {
		// trying to insert when filter has victim can create a second
		// victim!
		if (hasVictim && (table.insertToBucket(victim.i1, victim.tag) || table.insertToBucket(victim.i2, victim.tag))) {
			hasVictim = false;
		}
	}

	@VisibleForTesting
	boolean checkIsVictim(BucketAndTag tagToCheck) {
		checkNotNull(tagToCheck);
		if (hasVictim) {
			if (victim.tag == tagToCheck.tag && (tagToCheck.index == victim.i1 || tagToCheck.index == victim.i2)) {
				return true;
			}
		}
		return false;
	}

	public boolean mightContain(T item) {
		BucketAndTag pos = hasher.generate(item);
		int i1 = pos.index;
		int i2 = hasher.altIndex(pos.index, pos.tag);

		if (table.findTag(i1, i2, pos.tag)) {
			return true;
		}
		return checkIsVictim(pos);
	}

	public boolean delete(T item) {
		BucketAndTag pos = hasher.generate(item);
		int i1 = pos.index;
		int i2 = hasher.altIndex(pos.index, pos.tag);

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
			return this.hasher.equals(that.hasher) && this.table.equals(that.table) && this.count == that.count;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(hasher, table, count);
	}

	public CuckooFilter<T> copy() {
		return new CuckooFilter<>(hasher.copy(), table.copy(), count);
	}

}
