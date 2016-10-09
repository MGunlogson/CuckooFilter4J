
/*
   Copyright 2016 Mark Gunlogson

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/


import java.util.ArrayList;

import com.cuckooforjava.CuckooFilter;
import com.google.common.hash.Funnels;

public class BenchMultiThread {
	private static CuckooFilter<Integer> cuckoo;
	private static final int filterSize=100000000;
	private static final int numThreads=16;
	private static int totalItems=0;
	public static void main(String[] args) throws Exception 
	{
		cuckoo = new CuckooFilter.Builder<>(Funnels.integerFunnel(), filterSize)
				.withExpectedConcurrency(numThreads).build();
		final int segmentSize= (int)(filterSize*.8/numThreads);
		ArrayList<Thread> threads= new ArrayList<>();
		//set threads and have them insert items in part of int range that shouldn't overlap much
		for(int i=0;i<filterSize*0.8;i+=segmentSize)
		{
			Runnable putRunnable= new InsertRunnable(cuckoo,i,i+segmentSize);
			Thread thread = new Thread(putRunnable);
			threads.add(thread);
			totalItems=i+segmentSize;
		}
		//run them
		long startTime = System.nanoTime();
		for(Thread thread : threads)
		{
			thread.start();
		}
		//join back together
		for(Thread thread : threads)
		{
			thread.join();
		}
		long endTime = System.nanoTime();
		long totalTime = endTime-startTime;
		System.out.println("Inserts per second: "+ totalItems/(totalTime/1000000000.0));
		
	}
	

}
class InsertRunnable implements Runnable {
	  private final int startIndex;
	  private final int endIndex;
	  private final CuckooFilter<Integer> filter;
	  public InsertRunnable(CuckooFilter<Integer> cuckoo, int startIndex,int endIndex) {
	    this.startIndex = startIndex;
	    this.endIndex = endIndex;
	    this.filter=cuckoo;
	  }
	  @Override
	  public void run() {
		  for(int i=startIndex;i<endIndex;i++)
		  {
			  filter.put(i);
		  }
	  }
	}
