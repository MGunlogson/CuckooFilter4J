package jcfilter;

import com.google.common.hash.HashFunction;
	public class BucketTagGenerator
	{
		public class BucketAndTag {

			public final int BucketIndex;
			public final int Tag;
			private BucketAndTag(int BucketIndex,int Tag)
			{
				this.BucketIndex=BucketIndex;
				this.Tag=Tag;
			}
		}
		
		private final HashFunction HASHER;
		private final int NUM_BUCKETS;
		public BucketTagGenerator(HashFunction Hasher,int NumBuckets)
		{
			HASHER= Hasher;
			NUM_BUCKETS=NumBuckets;
					
		}
		public BucketAndTag Generate(byte[] item)
		{
			long HashVal = HASHER.hashBytes(item).asLong();
			//get two ints from the long without hashing again!
			int HighFour = (int)(HashVal >> 32);
			int BucketIndex= Math.abs(HighFour)%NUM_BUCKETS;
			int LowFour = (int)HashVal;
			return new BucketAndTag(BucketIndex,LowFour);
		}
	}

