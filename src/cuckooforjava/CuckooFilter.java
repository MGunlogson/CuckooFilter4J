package jcfilter;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import jcfilter.BucketTagGenerator.BucketAndTag;
import jcfilter.FilterTable.TagResult;
public class JCFilter {
	public final int MAX_INSERTION_ATTEMPTS;
	public final int BITS_PER_ITEM;
	public final HashFunction HASH_FUNCTION;
	public final int MAX_KEYS;
	public final int NUM_BUCKETS;
	public final static int DEFAULT_INSERTION_ATTEMPTS=500;
	public final int BUCKET_SIZE = 4;
	
	private FilterTable Table;
	private BucketTagGenerator BuckerHasher;
	enum ReturnStatus
	{
		Ok(0),
		NotFound(1),
		NotEnoughSpace(2),
		NotSupported(3);
	    private final int id;
		ReturnStatus(int id) { this.id = id; }
	    public int getValue() { return id; }
	}
	
	public JCFilter(int BitsPerItem,int MaxKeys)
	{
		this(BitsPerItem,MaxKeys ,GetGoodHash(),DEFAULT_INSERTION_ATTEMPTS);
	}
	public JCFilter(int BitsPerItem,int MaxKeys,HashFunction HashFunction)
	{
		this(BitsPerItem,MaxKeys ,HashFunction, DEFAULT_INSERTION_ATTEMPTS);
	}
	public JCFilter(int BitsPerItem,int MaxKeys,int InsertionAttempts)
	{
		this(BitsPerItem,MaxKeys ,GetGoodHash(), InsertionAttempts);
	}
	private static HashFunction GetGoodHash()
	{
		return Hashing.goodFastHash(64);
	}
	public JCFilter(int BitsPerItem,int MaxKeys ,HashFunction HashFunction,int InsertionAttempts)
	{
		MAX_INSERTION_ATTEMPTS = InsertionAttempts;
		BITS_PER_ITEM = BitsPerItem;
		HASH_FUNCTION = HashFunction;
		MAX_KEYS = MaxKeys;
		NUM_BUCKETS = (int)((1/0.96)*MAX_KEYS/BUCKET_SIZE);//calculated needed buckets at 96% fill rate
		if(NUM_BUCKETS<2) throw new IllegalArgumentException("filter must have at least 2 buckets, increase MaxKeys");
		if(BITS_PER_ITEM<2) throw new IllegalArgumentException("Invalid size, must be more than one bit per item");
        if(HASH_FUNCTION == null) throw new IllegalArgumentException("null Hash Function!");
        if(HASH_FUNCTION.bits()<64) throw new IllegalArgumentException("Hash Function less Than 64 bits not supported");
		if(MAX_INSERTION_ATTEMPTS<1)throw new IllegalArgumentException("Invalid retry attempts");
		BuckerHasher = new BucketTagGenerator(HASH_FUNCTION,NUM_BUCKETS);
	}
	public ReturnStatus Add(byte [] Item)
	{
		BucketAndTag pos = BuckerHasher.Generate(Item);
		int CurTag = pos.Tag;
		int CurIndex= pos.BucketIndex;
		TagResult res= Table.InsertTagIntoBucket(CurIndex, CurTag,false);
		if(res.Success)	return ReturnStatus.Ok;
		res = Table.InsertTagIntoBucket(AlternateIndex(CurTag,CurIndex), CurTag,true);
		//second position worked without kicking
		if(!res.Kicked)	return ReturnStatus.Ok;
		//if we kicked a tag we need to move it to its alternate position, possibly kicking another tag there
		//repeat the process until we succeed or run out of chances
		for(int i =0; i<=MAX_INSERTION_ATTEMPTS;i++)
		{
			//we always get the kicked tag returned if a tag is kicked to make room in alternate pos for our current tag
			CurTag = res.OldTag;
			CurIndex= AlternateIndex(CurIndex,CurTag);
			res= Table.InsertTagIntoBucket(CurIndex, CurTag,true);
			//alternate position worked without kicking
			if(!res.Kicked)	return ReturnStatus.Ok;
		}
		return ReturnStatus.NotEnoughSpace;
	}

	public ReturnStatus Contain(byte[] Item)
	{
		BucketAndTag pos = BuckerHasher.Generate(Item);
		int i1 = pos.BucketIndex;
		int i2 = AlternateIndex(pos.BucketIndex,pos.Tag);
		if(Table.FindTagInBuckets(i1,i2,pos.Tag))return ReturnStatus.Ok;
		else return ReturnStatus.NotFound;
	}
	public ReturnStatus Delete(byte[] Item)
	{
		BucketAndTag pos =  BuckerHasher.Generate(Item);
		int i1 = pos.BucketIndex;
		int i2 = AlternateIndex(pos.BucketIndex,pos.Tag);
		if(Table.DeleteTagInBucket(i1,pos.Tag) || Table.DeleteTagInBucket(i2,pos.Tag) )		return ReturnStatus.Ok;
		else return ReturnStatus.NotFound;
	}
	private int AlternateIndex(int BucketIndex,int Tag)
	{
        // 0x5bd1e995 hash constant from MurmurHash2
		// NOTE: This is pretty...interesting, perhaps we should run this through the main hash in the future?
		// for now we just keep since it's from original C++ implementation https://github.com/efficient/cuckoofilter
        return Math.abs((BucketIndex ^ (Tag * 0x5bd1e995)))%NUM_BUCKETS;
	}

}
