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

package com.github.mgunlogson.cuckoofilter4j;

import static org.junit.Assert.*;

import org.junit.Test;

import com.github.mgunlogson.cuckoofilter4j.BucketAndTag;
import com.github.mgunlogson.cuckoofilter4j.CuckooFilter;
import com.github.mgunlogson.cuckoofilter4j.Utils.Algorithm;
import com.github.mgunlogson.cuckoofilter4j.Utils.Victim;
import com.google.common.hash.Funnels;
import com.google.common.testing.ClassSanityTester;
import com.google.common.testing.EqualsTester;
import com.google.common.testing.SerializableTester;

class TestCuckooFilter {

	@Test(expected = IllegalArgumentException.class)
	void testInvalidArgsTooHighFp() {
		new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000)
				.withFalsePositiveRate(1).build();
	}

	//hash function not long enough
	@Test(expected = IllegalArgumentException.class)
	void testInvalidArgsShortHashFunction() {
		new CuckooFilter.Builder<>(Funnels.integerFunnel(), Integer.MAX_VALUE)
		.withFalsePositiveRate(0.01)
		.withHashAlgorithm(Algorithm.Murmur3_32).build();
	}

	@Test(expected = IllegalArgumentException.class)
	void testInvalidArgsZeroFp() {
		new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000)
		.withFalsePositiveRate(0.0).build();
	}

	@Test(expected = IllegalArgumentException.class)
	void testInvalidArgsNegItems() {
		new CuckooFilter.Builder<>(Funnels.integerFunnel(), -2000000).build();
	}

	@Test(expected = IllegalArgumentException.class)
	void testInvalidArgsNegFp() {
		new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000)
		.withFalsePositiveRate(-0.02).build();
	}
	@Test(expected = IllegalArgumentException.class)
	void testInvalidArgsZeroConcurrency() {
		new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000).withExpectedConcurrency(0).build();
	}
	@Test(expected = IllegalArgumentException.class)
	//not multiple of 2 concurrency
	void testInvalidArgsNotMult2Concurrency() {
		new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000).withExpectedConcurrency(10).build();
	}
	@Test
	//should just work
	void testConcurrencyWorks() {
		new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000).withExpectedConcurrency(16).build();
	}
	@Test
	 void testCreateDifferentHashLengths() {
		new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000).withHashAlgorithm(Algorithm.Murmur3_32).build();
		new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000).withHashAlgorithm(Algorithm.sipHash24).build();
		new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000).withHashAlgorithm(Algorithm.Murmur3_128).build();
		new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000).withHashAlgorithm(Algorithm.sha256).build();
	}
	
	
	@Test
	//should just work
	 void testNullHash() {
		new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000).build();
	}

	@Test
	 void sanityFalseNegative() {
		CuckooFilter<Integer> filter=new CuckooFilter.Builder<>(Funnels.integerFunnel(), 130000).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_32).build();
		// add them to filter
		for (int i = 0; i < 100000; i++) {
			// will return false if filter is full...should NOT be
			assertTrue(filter.put(i));
		}
		// check for false negatives
		int falseNegatives = 0;
		for (int i = 0; i < 100000; i++) {
			if (!filter.mightContain(i)) {
				falseNegatives++;
			}
		}
		assertTrue(falseNegatives + " false negatives detected", falseNegatives == 0);

	}
	@Test
	 void sanityApproimateCount() {
		CuckooFilter<Integer> filter=new CuckooFilter.Builder<>(Funnels.integerFunnel(), 130000).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_32).build();
		//fill buckets with duplicates, count along the way
		for(int i=0;i<8;i++)
		{
			assertTrue(filter.put(42));
			assertTrue(filter.approximateCount(42)==i+1);
		}
		//should fill victim
		assertTrue(filter.put(42));
		assertTrue(filter.approximateCount(42)==9);
		//should fail
		assertFalse(filter.put(42));
		//count should be the same
		assertTrue(filter.approximateCount(42)==9);
		//should delete victim and another pos
		assertTrue(filter.delete(42)&&filter.delete(42));
		//should be 7 copies now
		assertTrue(filter.approximateCount(42)==7);
		//loop delete rest
		for(int i=7;i>0;i--)
		{
			assertTrue(filter.delete(42));
			assertTrue(filter.approximateCount(42)==i-1);
		}
		//should be empty
		assertFalse(filter.mightContain(42));
	}
	
	

	@Test
	 void sanityOverFillFilter() {
		// make test set bigger than any size filter we're running
		for (int i = 1; i < 10; i++) {
			int filterKeys = 100000 * i;
			CuckooFilter<Integer> filter=new CuckooFilter.Builder<>(Funnels.integerFunnel(), filterKeys).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_32).build();

			// make a list of test values(all unique)
			// makes sure that filter can handle a 0.8 load factor before
			// insertion failure
			int countFailedAt = 0;
			while (true) {
				if (!filter.put(countFailedAt))
					break;
				countFailedAt++;
			}
			// make sure the filter reports as many as we actually put in
			assertTrue("Filter reports " + filter.getCount() + " when we actually added " + countFailedAt
					+ " items before failing", filter.getCount() == countFailedAt);

			// it's okay if filter is a bit bigger than we asked for, it should
			// never be more than twice as big plus 1 (due to numBuckets power
			// of 2 rounding)
			assertTrue("We were able to add " + countFailedAt + " keys to a filter that was only made to hold "
					+ filterKeys, countFailedAt <= (filterKeys * 2) + 1);

			// keep some tight error bounds to detect small anomalies...just
			// change if errors out too much
			assertTrue(
					"Load Factor only " + filter.getLoadFactor() + " for filter with " + filterKeys
							+ " capacity at first insertion failure. Expected more than 0.95",
					filter.getLoadFactor() > .95);
			assertTrue(
					"Load Factor " + filter.getLoadFactor() + " for filter with " + filterKeys
							+ " capacity at first insertion failure. Expected less than .995",
					filter.getLoadFactor() < 0.995);
		}
	}

	@Test
	 void sanityOverFillBucketMoreThan2B() {
		CuckooFilter<Integer> filter=new CuckooFilter.Builder<>(Funnels.integerFunnel(), 100000).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_32).build();
		int maxTries = 30;
		int failedAt = maxTries;
		for (int i = 0; i < maxTries; i++) {
			if (!filter.put(2)) {
				failedAt = i;
				break;
			}
		}
		assertTrue("Duplicate insert failed at " + failedAt + " Expected value is (2*BUCKET_SIZE)+victim cache = 9",
				failedAt == 9);
	}

	@Test
	 void sanityFailedDelete() {
		CuckooFilter<Integer> filter=new CuckooFilter.Builder<>(Funnels.integerFunnel(), 130000).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_32).build();

		// make a list of test values(all unique)
		int maxInsertedVal = 100000;
		for (int i = 0; i < maxInsertedVal; i++) {
			// will return false if filter is full...should NOT be
			assertTrue(filter.put(i));
		}
		// check for false deletes(if I can't delete something that's definitely
		// there)
		int falseDeletes = 0;
		for (int i = 0; i < maxInsertedVal; i++) {
			if (!filter.delete(i)) {
				falseDeletes++;
			}
		}
		assertTrue(falseDeletes + " false deletions detected", falseDeletes == 0);
	}

	@Test
	 void sanityFalseDeleteRate() {
		CuckooFilter<Integer> filter=new CuckooFilter.Builder<>(Funnels.integerFunnel(), 130000).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_32).build();
		int maxInsertedVal = 100000;
		for (int i = 0; i < maxInsertedVal; i++) {
			// will return false if filter is full...should NOT be
			assertTrue(filter.put(i));
		}
		// check for false delete rate(deleted something I didn't add
		// successfully)
		int falseDeletes = 0;
		// false delete rate should roughly match false positive rate
		int totalAttempts = 10000;
		maxInsertedVal += 1;
		for (int i = maxInsertedVal; i < totalAttempts + maxInsertedVal; i++) {
			if (filter.delete(i))
				falseDeletes++;
		}
		assertTrue(
				falseDeletes
						+ " false deletions detected. False delete rate is above 0.02 on filter with 0.01 false positive rate",
				(double) falseDeletes / totalAttempts < 0.02);

	}

	@Test
	 void sanityFalsePositiveRate() {
		CuckooFilter<Integer> filter=new CuckooFilter.Builder<>(Funnels.integerFunnel(), 130000).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_32).build();
		int maxInsertedVal = 100000;
		// make a list of test values(all unique)
		for (int i = 0; i < maxInsertedVal; i++) {
			// will return false if filter is full...should NOT be
			assertTrue(filter.put(i));
		}
		// check for false positive rate(contains something I didn't add)
		int falsePositives = 0;
		maxInsertedVal += 1;
		int totalAttempts = 100000;
		for (int i = maxInsertedVal; i < totalAttempts + maxInsertedVal; i++) {
			if (filter.mightContain(i))
				falsePositives++;
		}
		assertTrue((double) falsePositives / totalAttempts + " false positive rate is above limit",
				(double) falsePositives / totalAttempts < 0.02);

	}

	@Test
	 void sanityTestVictimCache() {
		CuckooFilter<Integer> filter=new CuckooFilter.Builder<>(Funnels.integerFunnel(), 130000).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_32).build();

		for (int i = 0; i < 9; i++) {
			assertTrue(filter.put(42));
		}
		assertTrue(filter.getCount() == 9);
		for (int i = 0; i < 9; i++) {
			assertTrue(filter.mightContain(42));
			assertTrue(filter.delete(42));
		}
		assertFalse(filter.delete(42));
		assertFalse(filter.mightContain(42));
		assertTrue(filter.getCount() == 0);
		// at this point victim cache is in use since both buckets for 42 are
		// full

	}

	@Test
	 void testVictimCacheTagComparison() {
		CuckooFilter<Integer> filter=new CuckooFilter.Builder<>(Funnels.integerFunnel(), 130000).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_32).build();
		filter.hasVictim = true;
		filter.victim = new Victim(1,2, 42);
		BucketAndTag test1 = new BucketAndTag(filter.victim.getI1(), 42);
		BucketAndTag test2 = new BucketAndTag(filter.victim.getI2(), 42);
		assertTrue(filter.checkIsVictim(test1));
		assertTrue(filter.checkIsVictim(test2));
	}

	@Test
	 void sanityFillDeleteAllAndCheckABunchOfStuff() {
		// test with different filter sizes
		for (int k = 1; k < 20; k++) {
			int filterKeys = 20000 * k;
			CuckooFilter<Integer> filter=new CuckooFilter.Builder<>(Funnels.integerFunnel(), filterKeys).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_32).build();
			// repeatedly fill and drain filter
			for (int j = 0; j < 3; j++) {
				stressFillDrainCheck(filter);
			}
		}
	}

	private void stressFillDrainCheck(CuckooFilter<Integer> filter) {
		int maxInsertedVal = 0;
		while (true) {
			// go until filter totally full
			if (!filter.put(maxInsertedVal)) {
				break;
			}
			maxInsertedVal++;
		}
		// everything we added should be there
		for (int i = 0; i < maxInsertedVal; i++) {
			assertTrue("filter doesn't contain " + i + " with " + maxInsertedVal + " total insertions",
					filter.mightContain(i));
		}
		// delete everything we just added
		// make three passes
		// first pass will get most
		// second pass should get any we deleted from "wrong" bucket
		// mathematically (almost)guaranteed to delete all items in filter if
		// it's working properly
		int deleteCount = 0;
		for (int i = 0; i < maxInsertedVal; i++) {
			if (filter.delete(i))
				deleteCount++;
		}
		// second pass
		for (int i = 0; i < maxInsertedVal; i++) {
			if (filter.delete(i))
				deleteCount++;
		}
		// did we get everything?
		assertTrue(maxInsertedVal == deleteCount);
		// does filter know it's empty?
		assertTrue(filter.getCount() == 0);

		// just to make sure everything is properly "gone"
		for (int i = 0; i < maxInsertedVal; i++) {
			assertFalse(filter.delete(i));
		}
		// and even more sure...should be zero false positives because filter is
		// totally empty
		for (int i = 0; i < maxInsertedVal; i++) {
			assertFalse(filter.mightContain(i));
		}
	}

	@Test
	 void testEquals() {
		CuckooFilter<Integer> partFull=new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_32).build();
		CuckooFilter<Integer> full=new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_32).build();
		for(int i=0;i<1000000;i++)
		{
			assertTrue(partFull.put(i));
		}
		for(int i=0;;i++)
		{
			if(!full.put(i))
				break;
		}
		new EqualsTester()
				.addEqualityGroup(partFull)
				.addEqualityGroup(full)
				.addEqualityGroup(new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_32).build())
				.addEqualityGroup(new CuckooFilter.Builder<>(Funnels.longFunnel(), 2000000).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_32).build())
				.addEqualityGroup(new CuckooFilter.Builder<>(Funnels.integerFunnel(), 1000000).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_32).build())
				.addEqualityGroup(new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000).withFalsePositiveRate(0.03).withHashAlgorithm(Algorithm.Murmur3_32).build())
				.addEqualityGroup(new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_128).build())
				.testEquals();
	}

	@Test
	 void testCopyEmpty() {
		CuckooFilter<Integer> filter = new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_32).build();
		CuckooFilter<Integer> filterCopy = filter.copy();
		assertTrue(filterCopy.equals(filter));
		assertNotSame(filter, filterCopy);
	}
	
	
	@Test
	 void testCopyPartFull() {
		CuckooFilter<Integer> filter = new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_32).build();
		for(int i=0;i<1000000;i++)
		{
			assertTrue(filter.put(i));
		}
		CuckooFilter<Integer> filterCopy = filter.copy();
		assertTrue(filterCopy.equals(filter));
		assertNotSame(filter, filterCopy);
	}
	
	
	@Test
	 void testCopyFull() {
		//totally full will test victim cache as well
		CuckooFilter<Integer> filter = new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_32).build();
		//fill until victim cache full
		for(int i=0;;i++)
		{
			// go until filter totally full
			if (!filter.put(i)) 
				break;
		}
		CuckooFilter<Integer> filterCopy = filter.copy();
		assertTrue(filterCopy.equals(filter));
		assertNotSame(filter, filterCopy);
	}
	

	@Test
	 void autoTestNulls() {
		// chose 15 for int so it passes checks
		new ClassSanityTester().setDefault(int.class, 15).setDefault(long.class, 15L).setDefault(double.class, 0.001).testNulls(CuckooFilter.class);
	}

	@Test
	 void testSerializeEmpty() {
		SerializableTester.reserializeAndAssert(
				new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_32).build());
	}
	
	@Test
	 void testSerializePartFull() {
		CuckooFilter<Integer> filter = new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_32).build();
		for(int i=0;i<1000000;i++)
		{
			assertTrue(filter.put(i));
		}
		SerializableTester.reserializeAndAssert(filter);
	}
	
	@Test
	 void testSerializeFull() {
		CuckooFilter<Integer> filter = new CuckooFilter.Builder<>(Funnels.integerFunnel(), 2000000).withFalsePositiveRate(0.01).withHashAlgorithm(Algorithm.Murmur3_32).build();
		for(int i=0;;i++)
		{
			if(!filter.put(i))
				break;
		}
		SerializableTester.reserializeAndAssert(filter);
	}

}
