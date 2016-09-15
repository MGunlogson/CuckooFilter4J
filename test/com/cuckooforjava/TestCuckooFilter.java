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

package com.cuckooforjava;

import static org.junit.Assert.*;

import org.junit.Test;

import com.cuckooforjava.CuckooFilter;
import com.cuckooforjava.SerializableSaltedHasher.Algorithm;
import com.google.common.hash.Funnels;
import com.google.common.testing.ClassSanityTester;
import com.google.common.testing.EqualsTester;
import com.google.common.testing.SerializableTester;

public class TestCuckooFilter {

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidArgs() {
		CuckooFilter.create(Funnels.integerFunnel(), 2000000, 1, Algorithm.Murmur3_32);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidArgs2() {
		CuckooFilter.create(Funnels.integerFunnel(), Integer.MAX_VALUE, 0.01, Algorithm.Murmur3_32);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidArgs3() {
		CuckooFilter.create(Funnels.integerFunnel(), 2000000, 0, Algorithm.Murmur3_32);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidArgs4() {
		CuckooFilter.create(Funnels.integerFunnel(), -2000000, 0.01, Algorithm.Murmur3_32);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidArgs5() {
		CuckooFilter.create(Funnels.integerFunnel(), 2000000, -0.01, Algorithm.Murmur3_32);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidArgs6() {
		CuckooFilter.create(Funnels.integerFunnel(), 200000000);
	}

	@Test
	public void sanityFalseNegative() {
		CuckooFilter<Integer> filter = CuckooFilter.create(Funnels.integerFunnel(), 130000, 0.01, Algorithm.Murmur3_32);
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
	public void sanityOverFillFilter() {
		// make test set bigger than any size filter we're running
		for (int i = 1; i < 10; i++) {
			int filterKeys = 100000 * i;
			CuckooFilter<Integer> filter = CuckooFilter.create(Funnels.integerFunnel(), filterKeys, 0.01,
					Algorithm.Murmur3_32);

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
	public void sanityOverFillBucketMoreThan2B() {
		CuckooFilter<Integer> filter = CuckooFilter.create(Funnels.integerFunnel(), 100000, 0.01, Algorithm.Murmur3_32);
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
	public void sanityFailedDelete() {
		CuckooFilter<Integer> filter = CuckooFilter.create(Funnels.integerFunnel(), 130000, 0.01, Algorithm.Murmur3_32);

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
	public void sanityFalseDeleteRate() {
		CuckooFilter<Integer> filter = CuckooFilter.create(Funnels.integerFunnel(), 130000, 0.01, Algorithm.Murmur3_32);
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
	public void sanityFalsePositiveRate() {
		CuckooFilter<Integer> filter = CuckooFilter.create(Funnels.integerFunnel(), 130000, 0.01, Algorithm.Murmur3_32);
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
	public void sanityTestVictimCache() {
		CuckooFilter<Integer> filter = CuckooFilter.create(Funnels.integerFunnel(), 130000, 0.01, Algorithm.Murmur3_32);

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
	public void testVictimCacheTagComparison() {
		CuckooFilter<Integer> filter = CuckooFilter.create(Funnels.integerFunnel(), 130000, 0.01, Algorithm.Murmur3_32);

		filter.hasVictim = true;
		filter.victim = filter.new Victim(1, 2);
		BucketAndTag test1 = new BucketAndTag(filter.victim.i1, 2);
		BucketAndTag test2 = new BucketAndTag(filter.victim.i2, 2);
		assertTrue(filter.checkIsVictim(test1));
		assertTrue(filter.checkIsVictim(test2));
	}

	@Test
	public void sanityFillDeleteAllAndCheckABunchOfStuff() {
		// test with different filter sizes
		for (int k = 1; k < 20; k++) {
			int filterKeys = 20000 * k;
			CuckooFilter<Integer> filter = CuckooFilter.create(Funnels.integerFunnel(), filterKeys, 0.01,
					Algorithm.Murmur3_32);
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
	public void testEquals() {
		new EqualsTester()
				// we don't test arg2 because numbuckets is only used to create
				// BitSet of estimated proper size.
				.addEqualityGroup(CuckooFilter.create(Funnels.integerFunnel(), 2000000, 0.01, Algorithm.Murmur3_32))
				.addEqualityGroup(CuckooFilter.create(Funnels.longFunnel(), 2000000, 0.01, Algorithm.Murmur3_32))
				.addEqualityGroup(CuckooFilter.create(Funnels.integerFunnel(), 1000000, 0.01, Algorithm.Murmur3_32))
				.addEqualityGroup(CuckooFilter.create(Funnels.integerFunnel(), 2000000, 0.03, Algorithm.Murmur3_32))
				.addEqualityGroup(CuckooFilter.create(Funnels.integerFunnel(), 2000000, 0.01, Algorithm.Murmur3_128))
				.testEquals();
	}

	@Test
	public void testCopy() {
		CuckooFilter<Integer> table = CuckooFilter.create(Funnels.integerFunnel(), 2000000, 0.01, Algorithm.Murmur3_32);
		CuckooFilter<Integer> tableCopy = table.copy();
		assertTrue(tableCopy.equals(table));
		assertNotSame(table, tableCopy);
	}

	@Test
	public void autoTestNulls() {
		// chose 15 for int so it passes checks
		new ClassSanityTester().setDefault(int.class, 15).setDefault(double.class, 0.001).testNulls(CuckooFilter.class);
	}

	@Test
	public void testSerialize() {
		SerializableTester.reserializeAndAssert(
				CuckooFilter.create(Funnels.integerFunnel(), 2000000, 0.01, Algorithm.Murmur3_32));
	}

}
