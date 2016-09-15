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

import com.google.common.hash.Funnel;
import com.google.common.math.DoubleMath;

import cuckooforjava.FilterTable.TagResult;
import cuckooforjava.SerializableSaltedHasher.Algorithm;

public final class CuckooFilter<T> implements Serializable {

	private static final long serialVersionUID = -1337735144654851942L;
	public static final int INSERTION_ATTEMPTS = 500;
	public static final int BUCKET_SIZE = 4;
	// make sure to update getNeededBitsForFpRate() if changing this... then
	// again don't change this
	public static final double LOAD_FACTOR = 0.955;
	public static final double DEFAULT_FP_RATE = 0.01;

	private FilterTable table;
	private IndexTagCalc<T> itemHasher;
	private int count;
	// when the filter becomes completely full, the last item that fails to be
	// repositioned will be left without a home
	private BucketAndTag fullFilterVictim;
	private boolean filterHasVictim;

	private CuckooFilter(IndexTagCalc<T> hasher, FilterTable table, int count) {
		this.itemHasher = hasher;
		this.table = table;
		this.count = count;
	}

	public static <T> CuckooFilter<T> create(Funnel<? super T> funnel, int maxKeys) {
		return create(funnel, maxKeys, DEFAULT_FP_RATE, Algorithm.Murmur3_32);
	}

	public static <T> CuckooFilter<T> create(Funnel<? super T> funnel, int maxKeys, double fpp) {
		return create(funnel, maxKeys, fpp, Algorithm.Murmur3_32);
	}

	public static <T> CuckooFilter<T> create(Funnel<? super T> funnel, int maxKeys, Algorithm hashAlgorithm) {
		return create(funnel, maxKeys, DEFAULT_FP_RATE, hashAlgorithm);
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
		IndexTagCalc<T> hasher = new IndexTagCalc<T>(hashAlgorithm, funnel, numBuckets, tagBits);
		FilterTable filtertbl = FilterTable.create(tagBits, numBuckets, maxKeys);
		return new CuckooFilter<T>(hasher, filtertbl, 0);

	}

	private static int getBitsPerItemForFpRate(double fpProb) {
		/*
		 * equation from Cuckoo Filter: Practically Better Than Bloom Bin Fan,
		 * David G. Andersen, Michael Kaminsky , Michael D. Mitzenmacher NOTE:
		 * depends on LOAD FACTOR!!
		 */
		return DoubleMath.roundToInt(DoubleMath.log2((1 / fpProb) + 3) / LOAD_FACTOR, RoundingMode.UP);
	}

	private static int getBucketsNeeded(int maxKeys) {
		/*
		 * we force a power-of-two bucket count so our hash functions for bucket
		 * index can do hashBits%numBuckets and get a randomly distributed index
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
		// can return more than one if running above design load factor!!
		return count / (itemHasher.getNumBuckets() * (double) BUCKET_SIZE);
	}

	public int getStorageSize() {
		return table.getStorageSize();
	}

	public boolean put(T item) {
		BucketAndTag pos = itemHasher.generate(item);
		int curTag = pos.tag;
		int curIndex = pos.bucketIndex;
		TagResult res = table.insertTagIntoBucket(curIndex, curTag, false);
		if (res.isSuccess()) {
			count++;
			return true;
		}
		// try the other bucket for this tag
		curIndex = itemHasher.alternateIndex(curIndex, curTag);
		res = table.insertTagIntoBucket(curIndex, curTag, true);
		// second position worked without kicking
		if (res.getOldTag() == 0) {
			count++;
			return true;
		}
		// don't do insertion loop if victim slot is already filled...we could
		// create a second
		if (filterHasVictim && !tryInsertVictim())
			return false;
		// if we kicked a tag we need to move it to its alternate position,
		// possibly kicking another tag there
		// repeat the process until we succeed or run out of chances
		for (int i = 0; i <= INSERTION_ATTEMPTS; i++) {
			// we always get the kicked tag returned if a tag is kicked to make
			// room in alternate pos for our current tag
			curTag = res.getOldTag();
			curIndex = itemHasher.alternateIndex(curIndex, curTag);
			res = table.insertTagIntoBucket(curIndex, curTag, true);
			// alternate position worked without kicking
			if (res.getOldTag() == 0) {
				count++;
				return true;
			}
		}
		// get here if we couldn't insert and have a random tag floating around
		// out of table
		filterHasVictim = true;
		fullFilterVictim = new BucketAndTag(curIndex, res.getOldTag());
		count++;// technically victim is in table
		return true;// technically victim still got put somewhere
	}

	private boolean tryInsertVictim() {
		// trying to insert when filter has victim can create a second victim!
		if (table.insertTagIntoBucket(fullFilterVictim.bucketIndex, fullFilterVictim.tag, false).isSuccess()) {
			filterHasVictim = false;
			return true;
		} else {
			// try alt bucket??
			if (table.insertTagIntoBucket(itemHasher.alternateIndex(fullFilterVictim.bucketIndex, fullFilterVictim.tag),
					fullFilterVictim.tag, false).isSuccess()) {
				filterHasVictim = false;
				return true;
			}
		}
		return false;
	}

	private boolean checkIsVictim(BucketAndTag tagToCheck) {
		if (fullFilterVictim.tag == tagToCheck.tag) {
			if (tagToCheck.bucketIndex == fullFilterVictim.bucketIndex || itemHasher
					.alternateIndex(tagToCheck.bucketIndex, tagToCheck.tag) == fullFilterVictim.bucketIndex)
				return true;
		}
		return false;
	}

	public boolean mightContain(T item) {
		BucketAndTag pos = itemHasher.generate(item);
		int i1 = pos.bucketIndex;
		int i2 = itemHasher.alternateIndex(pos.bucketIndex, pos.tag);
		if (table.findTagInBuckets(i1, i2, pos.tag)) {
			return true;
		}
		if (filterHasVictim) {
			return checkIsVictim(pos);
		}
		return false;
	}

	public boolean delete(T item) {
		BucketAndTag pos = itemHasher.generate(item);
		int i1 = pos.bucketIndex;
		int i2 = itemHasher.alternateIndex(pos.bucketIndex, pos.tag);

		if (table.deleteTagInBucket(i1, pos.tag) || table.deleteTagInBucket(i2, pos.tag)) {
			count--;
			if (filterHasVictim)
				tryInsertVictim();// might as well try to insert again
			return true;
		}
		if (filterHasVictim && checkIsVictim(pos)) {
			filterHasVictim = false;
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
			return this.itemHasher.equals(that.itemHasher) && this.table.equals(that.table) && this.count == that.count;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(itemHasher, table, count);
	}

	public CuckooFilter<T> copy() {
		return new CuckooFilter<T>(itemHasher.copy(), table.copy(), count);
	}

}
