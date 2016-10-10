package com.github.mgunlogson.cuckoofilter4j;

import org.junit.Test;

public class TestCuckooFilterConcurrent {
	@Test
	public void putMultiThread()
	{
		//starts some threads
		//put a bunch of stuff into filter using all threads
		
		//single thread check to make sure everything is there
		
		//is count correct?
		
	}
	@Test
	public void mightContainMultiThread()
	{
		//put a bunch of stuff in single-threaded
		
		//multi-thread cheack mightcontain
		
	}
	
	@Test
	public void deleteMultiThread()
	{
		//put a bunch of stuff in single-threaded
		
		//multi-threaded delete all items
		
		//single thread check... is everything gone?
		
		//is count correct?
	}
	
	//over-fill filter(mostly test victim locking)
	@Test
	public void overFillMultiThread()
	{
		//put a bunch of stuff in multi-threaded until filter full in all threads
		
		//multi-thread make sure everything added is there
		
		//multi-thread count check
		
		//delete all multi-threaded

		//make sure filter is empty.. multi-thread contain and count check
	}
	
	
	
	

}
