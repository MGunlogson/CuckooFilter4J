package cuckooforjava;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.Random;

class FilterTable {

	private BitSet memBlock;
	private final int bitsPerTag;
	private final int bitsPerBucket;
	private Random rando;
	private final ByteBuffer intByteConverter;
	private final int tagSizeInBytes;

	public FilterTable(int tagBits) {
		bitsPerTag = tagBits;
		memBlock = new BitSet();
		bitsPerBucket = CuckooFilter.BUCKET_SIZE * bitsPerTag;
		tagSizeInBytes = (int) Math.ceil((double) bitsPerTag / 8);
		intByteConverter = ByteBuffer.allocate(tagSizeInBytes);
		// MAKE ===SURE=== THE ByteBuffer IS LITTLE ENDIAN SO TAG READ OPERATION
		// FROM BitSet->ByteBuffer->int DOES NOT EXPLODE
		intByteConverter.order(ByteOrder.LITTLE_ENDIAN);
	}

	class TagResult {
		private final boolean success;
		private final int oldTag;
		private final boolean kicked;

		public TagResult(boolean success, int oldTag, boolean kicked) {
			this.success = success;
			this.kicked = kicked;
			this.oldTag = oldTag;
		}

		public boolean isSuccess() {
			return success;
		}

		public int getOldTag() {
			return oldTag;
		}

		public boolean isKicked() {
			return kicked;
		}
	}

	public TagResult insertTagIntoBucket(int bucketIndex, int tag, boolean kickOnFull) {

		for (int i = 0; i < CuckooFilter.BUCKET_SIZE; i++) {
			if (readTag(bucketIndex, i) == 0) {
				writeTag(bucketIndex, i, tag);
				return new TagResult(true, 0, false);
			}
		}
		if (kickOnFull) {
			// only get here if bucket is full :(
			// pick a random victim to kick from bucket if full
			// insert our new item and hand back kicked one
			int randomBucketPosition = rando.nextInt() % CuckooFilter.BUCKET_SIZE;
			int oldTag = readTag(bucketIndex, randomBucketPosition);
			assert oldTag!=0;
			writeTag(bucketIndex, randomBucketPosition, tag);
			return new TagResult(true, oldTag, true);
		}
		return new TagResult(false, 0, false);
	}

	public boolean findTagInBuckets(int bucketIndex1, int bucketIndex2, int tag) {
		for (int i = 0; i < CuckooFilter.BUCKET_SIZE; i++) {
			if ((readTag(bucketIndex1, i) == tag) || (readTag(bucketIndex2, i) == tag))
				return true;
		}
		return false;
	}

	public boolean deleteTagInBucket(int bucketIndex, int tag) {
		for (int i = 0; i < CuckooFilter.BUCKET_SIZE; i++) {
			if (readTag(bucketIndex, i) == tag) {
				writeTag(bucketIndex, i, 0);
				return true;
			}
		}
		return false;
	}

	private int readTag(int bucketIndex, int posInBucket) {
		int memOffset = (bucketIndex * bitsPerBucket) + posInBucket;
		byte[] tagBytes = memBlock.get(memOffset, memOffset + bitsPerTag).toByteArray();
		intByteConverter.put(tagBytes);
		intByteConverter.flip();
		int tag = intByteConverter.getInt();
		intByteConverter.clear();
		return tag;
	}

	private void writeTag(int bucketIndex, int posInBucket, int tag) {
		int memOffset = (bucketIndex * bitsPerBucket) + posInBucket;
		intByteConverter.putInt(tag);
		intByteConverter.flip();
		BitSet tagBits = BitSet.valueOf(intByteConverter);
		intByteConverter.clear();
		// BIT BANGIN YEAAAARRHHHGGGHHH
		for (int i = 0; i < bitsPerTag; i++) {
			memBlock.set(memOffset + i, tagBits.get(i));
		}
	}

}
