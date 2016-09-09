package jcfilter;

import java.util.BitSet;
import java.util.Random;

import com.google.common.primitives.Ints;

public class FilterTable {

	private BitSet MemBlock;
	private final int TAGS_PER_BUCKET;
	private final int TAG_BITS;
	private final int BUCKET_SIZE_BITS;
	private Random Rando;
	public FilterTable(int TagsPerBucket,int TagBits)
	{
		TAGS_PER_BUCKET=TagsPerBucket;
		TAG_BITS = TagBits;
		MemBlock = new BitSet();
		BUCKET_SIZE_BITS=TAGS_PER_BUCKET*TAG_BITS;
	}
	
	public class TagResult 
	{
		public boolean Success;
		public int OldTag;
		public boolean Kicked;
		public TagResult(boolean Success,int OldTag,boolean Kicked)
		{
			this.Success = Success;
			this.Kicked = Kicked;
			this.OldTag = OldTag;
		}
	}

	public TagResult InsertTagIntoBucket(int BucketIndex,int Tag,boolean KickOnFull)
	{
		
        for (int i = 0; i < TAGS_PER_BUCKET; i++ )
        {
            if (ReadTag(BucketIndex, i) == 0)
            {
                WriteTag(BucketIndex, i, Tag);
                return new TagResult(true, 0,false);
            }
        }
        if(KickOnFull)
        {
	        //only get here if bucket is full :(
	      	//pick a random victim to kick from bucket if full
	        //insert our new item and hand back kicked one 
	         int RandomBucketPosition =Rando.nextInt() % TAGS_PER_BUCKET;
	         int OldTag = ReadTag(BucketIndex, RandomBucketPosition);
	         WriteTag(BucketIndex, RandomBucketPosition, Tag);
	         return new TagResult(true,OldTag,true);
        }
        return new TagResult(false,0,false);
	}
	public boolean FindTagInBuckets(int BucketIndex1,int BucketIndex2,int Tag)
	{
             for (int i = 0; i < TAGS_PER_BUCKET; i++ )
             {
                 if ((ReadTag(BucketIndex1, i) == Tag) || (ReadTag(BucketIndex2,i) == Tag))
                     return true;
             }
             return false;
	}
	public boolean DeleteTagInBucket(int BucketIndex, int Tag)
	{
		   for (int i = 0; i < TAGS_PER_BUCKET; i++ )
		   {
               if (ReadTag(BucketIndex, i) == Tag) 
               {
                   WriteTag(BucketIndex, i, 0);
                   return true;
               }
           }
           return false;
	}
	private int ReadTag(int BucketIndex,int PosInBucket)
	{
		int MemOffset = (BucketIndex*BUCKET_SIZE_BITS)+PosInBucket;
		byte[] TagBytes = MemBlock.get(MemOffset, MemOffset+TAG_BITS).toByteArray();
		return Ints.fromByteArray(TagBytes);
	}
	private void WriteTag(int BucketIndex, int PosInBucket,int Tag )
	{
		int MemOffset = (BucketIndex*BUCKET_SIZE_BITS)+PosInBucket;
		
		byte[] TagBitMask;
		TagBitMask=Ints.toByteArray(Tag);
		BitSet TagBits = BitSet.valueOf(TagBitMask);
		//BIT BANGIN
		for(int i=0;i<TAG_BITS;i++)
		{
			MemBlock.set(MemOffset+i, TagBits.get(i));
		}
	}
	
	
}
