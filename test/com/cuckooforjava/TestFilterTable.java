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

package com.cuckooforjava;

import static org.junit.Assert.*;

import java.util.HashSet;

import org.junit.Test;

import com.google.common.testing.ClassSanityTester;
import com.google.common.testing.EqualsTester;
import com.google.common.testing.SerializableTester;

public class TestFilterTable {

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidArgs() {
		FilterTable.create(0, 100);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidArgs2() {
		FilterTable.create(5, 0);
	}
	//tag too short
	@Test(expected = IllegalArgumentException.class)
	public void testInvalidArgs3() {
		FilterTable.create(4, 100);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testTagTooBig() {
		FilterTable.create(60, 100);
	}

	@Test
	public void testSimpleReadWriteTag() {
		FilterTable table = FilterTable.create(12, 1000);
		int testTag = 0b00000000000000000000000000011111;
		for (int posInBucket = 0; posInBucket < 4; posInBucket++) {
			for (int bucket = 0; bucket < 1000; bucket++) {
				table.writeTagNoClear(bucket, posInBucket, testTag);
			}
			for (int bucket = 0; bucket < 1000; bucket++) {
				assertTrue(table.readTag(bucket, posInBucket) == testTag);
			}
		}
	}

	@Test
	public void testSimpleDeleteTag() {
		FilterTable table = FilterTable.create(12, 1000);
		int testTag = 0b00000000000000000000000000011111;
		// fill all bucket positions
		for (int posInBucket = 0; posInBucket < 4; posInBucket++) {
			for (int bucket = 0; bucket < 1000; bucket++) {
				// switch tag around a bit on each bucket insert
				int tagMutate = testTag >>> posInBucket;
				table.writeTagNoClear(bucket, posInBucket, tagMutate);
			}
		}
		// now delete all
		for (int posInBucket = 0; posInBucket < 4; posInBucket++) {
			for (int bucket = 0; bucket < 1000; bucket++) {
				// switch tag around a bit on each bucket insert
				int tagMutate = testTag >>> posInBucket;
				table.deleteFromBucket(bucket, tagMutate);
			}
		}
		// should be empty
		for (int posInBucket = 0; posInBucket < 4; posInBucket++) {
			for (int bucket = 0; bucket < 1000; bucket++) {
				assertTrue(table.readTag(bucket, posInBucket) == 0);
			}
		}

	}

	@Test
	public void testSimpleFindTag() {
		FilterTable table = FilterTable.create(12, 1000);
		int testTag = 0b00000000000000000000000000011111;
		table.writeTagNoClear(1, 2, testTag);
		assertFalse(table.findTag(2, 3, testTag));
		assertTrue(table.findTag(1, 3, testTag));
		assertTrue(table.findTag(3, 1, testTag));
		table.writeTagNoClear(2, 2, testTag);
		assertTrue(table.findTag(1, 2, testTag));
	}

	@Test
	public void testOverFillBucket() {
		int testTag = 0b00000000000000000000000000011111;
		FilterTable table = FilterTable.create(12, 1000);
		// buckets can hold 4 tags
		assertTrue(table.insertToBucket(5, testTag));
		assertTrue(table.insertToBucket(5, testTag));
		assertTrue(table.insertToBucket(5, testTag));
		assertTrue(table.insertToBucket(5, testTag));
		assertFalse(table.insertToBucket(5, testTag));
	}

	@Test
	public void testTagSwap() {
		int testTag = 0b00000000000000000000000000011111;
		FilterTable table = FilterTable.create(12, 1000);
		// buckets can hold 4 tags
		assertTrue(table.insertToBucket(5, testTag));
		assertTrue(table.insertToBucket(5, testTag));
		assertTrue(table.insertToBucket(5, testTag));
		assertTrue(table.insertToBucket(5, testTag));
		assertFalse(table.insertToBucket(5, testTag));
		// make sure table will give me a tag and swap
		long swap = table.swapRandomTagInBucket(5, 6);
		assertTrue("swapped tag is " + swap + " expected " + testTag, swap == testTag);
		assertTrue(table.findTag(5, 1, 6));
		assertTrue(table.findTag(1, 5, 6));
	}

	@Test
	public void testTagSwap2() {
		FilterTable table = FilterTable.create(12, 1000);
		// buckets can hold 4 tags
		assertTrue(table.insertToBucket(5, 1L));
		assertTrue(table.insertToBucket(5, 2L));
		assertTrue(table.insertToBucket(5, 3L));
		assertTrue(table.insertToBucket(5, 4L));
		// make sure table will give me a tag and swap
		long swap = 5;
		for (int i = 0; i < 1000; i++) {
			swap = table.swapRandomTagInBucket(5, swap);
		}
		HashSet<Long> tagVals = new HashSet<>();
		tagVals.add(swap);
		tagVals.add(table.readTag(5, 0));
		tagVals.add(table.readTag(5, 1));
		tagVals.add(table.readTag(5, 2));
		tagVals.add(table.readTag(5, 3));
		assertTrue(tagVals.size() == 5);
		assertTrue(tagVals.contains(1L));
		assertTrue(tagVals.contains(2L));
		assertTrue(tagVals.contains(3L));
		assertTrue(tagVals.contains(4L));
		assertTrue(tagVals.contains(5L));
	}

	@Test
	public void testBitBleedWithinBucket() {
		int canaryTag = 0b11111111111111111111111111111111;
		FilterTable table = FilterTable.create(12, 1000);
		// buckets can hold 4 tags
		table.writeTagNoClear(5, 0, canaryTag);
		table.writeTagNoClear(5, 2, canaryTag);
		assertTrue(table.readTag(5, 1) == 0);
		assertTrue(table.readTag(5, 3) == 0);
	}
	
	
	@Test
	public void testDeleteCorrectBits() {
		int canaryTag = 0b111111111111;
		FilterTable table = FilterTable.create(12, 1000);
		// buckets can hold 4 tags
		table.writeTagNoClear(5, 0, canaryTag);
		table.writeTagNoClear(5, 1, canaryTag);
		table.writeTagNoClear(5, 2, canaryTag);
		table.writeTagNoClear(5, 3, canaryTag);
		table.deleteTag(5, 1);
		table.deleteTag(5, 2);
		assertTrue(table.readTag(5, 1) == 0);
		assertTrue(table.readTag(5, 2) == 0);
		assertTrue(table.readTag(5, 0) == canaryTag);
		assertTrue(table.readTag(5, 3) == canaryTag);
	}

	@Test
	public void testBitBleedBetweenBuckets() {
		int canaryTag = 0b11111111111111111111111111111111;
		FilterTable table = FilterTable.create(12, 1000);
		// buckets can hold 4 tags
		table.writeTagNoClear(5, 0, canaryTag);
		table.writeTagNoClear(5, 3, canaryTag);
		// should be directly adjacent to positions we filled
		assertTrue(table.readTag(4, 3) == 0);
		assertTrue(table.readTag(6, 0) == 0);
	}

	@Test
	public void testEquals() {
		new EqualsTester()
				.addEqualityGroup(FilterTable.create(12, 1000))
				.addEqualityGroup(FilterTable.create(13, 1000))
				.addEqualityGroup(FilterTable.create(12, 2000)).testEquals();
	}

	@Test
	public void testCopy() {
		FilterTable table = FilterTable.create(12, 1000);
		FilterTable tableCopy = table.copy();
		assertTrue(tableCopy.equals(table));
		assertNotSame(table, tableCopy);
	}

	@Test
	public void autoTestNulls() {
		// chose 15 for int so it passes checks
		new ClassSanityTester().setDefault(int.class, 15).testNulls(FilterTable.class);
	}

	@Test
	public void testSerialize() {
		SerializableTester.reserializeAndAssert(FilterTable.create(12, 1000));
	}

}
