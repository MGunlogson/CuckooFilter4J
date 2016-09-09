package cuckooforjava;

import com.google.common.hash.HashFunction;
import com.google.common.primitives.Ints;

class BucketTagGenerator {
	class BucketAndTag {

		public final int bucketIndex;
		public final int tag;

		private BucketAndTag(int bucketIndex, int tag) {
			this.bucketIndex = bucketIndex;
			this.tag = tag;
		}
	}

	private final HashFunction hasher;
	private final int numBuckets;

	public BucketTagGenerator(HashFunction hasher, int numBuckets) {
		this.hasher = hasher;
		this.numBuckets = numBuckets;

	}

	public BucketAndTag generate(byte[] item) {
		int tag;
		int bucketIndex;
		NEED TO FIX THIS SO TAG CANNOT BE GENERATED AS ZERO
		byte[] hashVal = hasher.hashBytes(item).asBytes();
		// get two ints from the long without hashing again!
		bucketIndex = higherBytesToInt(hashVal);
		// flip bits if negative---force positive number in bucket index
		// calc
		if (bucketIndex < 0)
			bucketIndex = ~bucketIndex;
		bucketIndex = bucketIndex % numBuckets;
		assert bucketIndex >= 0 && bucketIndex < numBuckets;
		// low chance of this happening...but zero is our EMPTY BUCKET
		// identifier
		// just regen everything if we get zero for tag
		tag = lowBytesToInt(hashVal);
		return new BucketAndTag(bucketIndex, tag);
	}

	private int lowBytesToInt(byte[] bytes) {
		return Ints.fromBytes(bytes[3], bytes[2], bytes[1], bytes[0]);
	}

	private int higherBytesToInt(byte[] bytes) {
		return Ints.fromBytes(bytes[8], bytes[7], bytes[6], bytes[5]);
	}
}
