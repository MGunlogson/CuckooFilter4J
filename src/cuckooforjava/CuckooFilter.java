package cuckooforjava;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import cuckooforjava.BucketTagGenerator.BucketAndTag;
import cuckooforjava.FilterTable.TagResult;

public class CuckooFilter {
	public final int maxInsertionAttempts;
	public final int bitsPerItem;
	public final HashFunction hashFunction;
	public final int maxKeys;
	public final int numBuckets;
	public static final int DEFAULT_INSERTION_ATTEMPTS = 500;
	public static final int BUCKET_SIZE = 4;
	public static final int MIN_HASH_BITS = 64;
	private FilterTable table;
	private BucketTagGenerator bucketHasher;

	public enum ReturnStatus {
		OK(0), NOT_FOUND(1), INSUF_SPACE(2), NOT_SUPPORTED(3);
		private final int id;

		ReturnStatus(int id) {
			this.id = id;
		}

		public int getValue() {
			return id;
		}
	}

	public CuckooFilter(int bitsPerItem, int maxKeys) {
		this(bitsPerItem, maxKeys, getGoodHash(), DEFAULT_INSERTION_ATTEMPTS);
	}

	public CuckooFilter(int bitsPerItem, int maxKeys, HashFunction hashFunction) {
		this(bitsPerItem, maxKeys, hashFunction, DEFAULT_INSERTION_ATTEMPTS);
	}

	public CuckooFilter(int bitsPerItem, int maxKeys, int insertionAttempts) {
		this(bitsPerItem, maxKeys, getGoodHash(), insertionAttempts);
	}

	public CuckooFilter(int bitsPerItem, int maxKeys, HashFunction hashFunction, int insertionAttempts) {
		this.maxInsertionAttempts = insertionAttempts;
		this.bitsPerItem = bitsPerItem;
		this.hashFunction = hashFunction;
		this.maxKeys = maxKeys;
		numBuckets = (int) ((1 / 0.96) * maxKeys / BUCKET_SIZE);// calculated
																// needed
																// buckets at
																// 96% fill rate
		checkArgument(numBuckets > 1, "numBuckets (%s) must be > 1, increase maxKeys", numBuckets);
		checkArgument(bitsPerItem > 1, "bitsPerItem (%s) must be > 1", bitsPerItem);
		checkNotNull(hashFunction);
		checkArgument(hashFunction.bits() >= 64,
				"hash function length (%s) must be >= 64. Choose a different hash function", hashFunction.bits());
		checkArgument(maxInsertionAttempts > 0, "maxInsertionAttempts (%s) must be > 0", maxInsertionAttempts);

		bucketHasher = new BucketTagGenerator(hashFunction, numBuckets);
	}

	private static HashFunction getGoodHash() {
		return Hashing.goodFastHash(MIN_HASH_BITS);
	}

	public ReturnStatus add(byte[] item) {
		BucketAndTag pos = bucketHasher.generate(item);
		int curTag = pos.tag;
		int curIndex = pos.bucketIndex;
		TagResult res = table.insertTagIntoBucket(curIndex, curTag, false);
		if (res.isSuccess())
			return ReturnStatus.OK;
		res = table.insertTagIntoBucket(alternateIndex(curTag, curIndex), curTag, true);
		// second position worked without kicking
		if (!res.isKicked())
			return ReturnStatus.OK;
		// if we kicked a tag we need to move it to its alternate position,
		// possibly kicking another tag there
		// repeat the process until we succeed or run out of chances
		for (int i = 0; i <= maxInsertionAttempts; i++) {
			// we always get the kicked tag returned if a tag is kicked to make
			// room in alternate pos for our current tag
			curTag = res.getOldTag();
			curIndex = alternateIndex(curIndex, curTag);
			res = table.insertTagIntoBucket(curIndex, curTag, true);
			// alternate position worked without kicking
			if (!res.isKicked())
				return ReturnStatus.OK;
		}
		return ReturnStatus.INSUF_SPACE;
	}

	public ReturnStatus contain(byte[] item) {
		BucketAndTag pos = bucketHasher.generate(item);
		int i1 = pos.bucketIndex;
		int i2 = alternateIndex(pos.bucketIndex, pos.tag);
		if (table.findTagInBuckets(i1, i2, pos.tag))
			return ReturnStatus.OK;
		else
			return ReturnStatus.NOT_FOUND;
	}

	public ReturnStatus delete(byte[] item) {
		BucketAndTag pos = bucketHasher.generate(item);
		int i1 = pos.bucketIndex;
		int i2 = alternateIndex(pos.bucketIndex, pos.tag);
		if (table.deleteTagInBucket(i1, pos.tag) || table.deleteTagInBucket(i2, pos.tag))
			return ReturnStatus.OK;
		else
			return ReturnStatus.NOT_FOUND;
	}

	private int alternateIndex(int bucketIndex, int tag) {
		/*
		 * 0x5bd1e995 hash constant from MurmurHash2...interesting for now we
		 * also used in c impl
		 * https://github.com/efficient/cuckoofilter/
		 *  TODO: maybe we should just run the hash algorithm again?
		 */
		int reHashed = bucketIndex ^ (tag * 0x5bd1e995);
		// flip bits if negative,force positive bucket index
		if (reHashed < 0)
			reHashed = ~reHashed;
		return reHashed % numBuckets;
	}

}
