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

package cuckooforjava;

import static org.junit.Assert.*;

import java.util.HashSet;

import org.junit.Test;

import com.google.common.testing.ClassSanityTester;
import com.google.common.testing.EqualsTester;
import com.google.common.testing.SerializableTester;

public class TestFilterTable {

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidArgs() {
		FilterTable.create(0, 100, 10000);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidArgs2() {
		FilterTable.create(5, 0, 10000);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidArgs3() {
		FilterTable.create(5, 100, 0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testTagTooBig() {
		FilterTable.create(30, 100, 10000);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFilterTooBig() {
		FilterTable.create(28, 1000000, 200000000);
	}

	@Test
	public void testSimpleReadWriteTag() {
		FilterTable table = FilterTable.create(12, 1000, 2000000);
		int testTag = 0b00000000000000000000000000011111;
		for (int posInBucket = 0; posInBucket < 4; posInBucket++) {
			for (int bucket = 0; bucket < 1000; bucket++) {
				table.writeTag(bucket, posInBucket, testTag);
			}
			for (int bucket = 0; bucket < 1000; bucket++) {
				assertTrue(table.readTag(bucket, posInBucket) == testTag);
			}
		}
	}

	@Test
	public void testSimpleDeleteTag() {
		FilterTable table = FilterTable.create(12, 1000, 2000000);
		int testTag = 0b00000000000000000000000000011111;
		// fill all bucket positions
		for (int posInBucket = 0; posInBucket < 4; posInBucket++) {
			for (int bucket = 0; bucket < 1000; bucket++) {
				// switch tag around a bit on each bucket insert
				int tagMutate = testTag >>> posInBucket;
				table.writeTag(bucket, posInBucket, tagMutate);
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
		FilterTable table = FilterTable.create(12, 1000, 2000000);
		int testTag = 0b00000000000000000000000000011111;
		table.writeTag(1, 2, testTag);
		assertFalse(table.findTag(2, 3, testTag));
		assertTrue(table.findTag(1, 3, testTag));
		assertTrue(table.findTag(3, 1, testTag));
		table.writeTag(2, 2, testTag);
		assertTrue(table.findTag(1, 2, testTag));
	}

	@Test
	public void testOverFillBucket() {
		int testTag = 0b00000000000000000000000000011111;
		FilterTable table = FilterTable.create(12, 1000, 2000000);
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
		FilterTable table = FilterTable.create(12, 1000, 2000000);
		// buckets can hold 4 tags
		assertTrue(table.insertToBucket(5, testTag));
		assertTrue(table.insertToBucket(5, testTag));
		assertTrue(table.insertToBucket(5, testTag));
		assertTrue(table.insertToBucket(5, testTag));
		assertFalse(table.insertToBucket(5, testTag));
		// make sure table will give me a tag and swap
		int swap = table.swapRandomTagInBucket(5, 6);
		assertTrue("swapped tag is " + swap + " expected " + testTag, swap == testTag);
		assertTrue(table.findTag(5, 1, 6));
		assertTrue(table.findTag(1, 5, 6));
	}

	@Test
	public void testTagSwap2() {
		FilterTable table = FilterTable.create(12, 1000, 2000000);
		// buckets can hold 4 tags
		assertTrue(table.insertToBucket(5, 1));
		assertTrue(table.insertToBucket(5, 2));
		assertTrue(table.insertToBucket(5, 3));
		assertTrue(table.insertToBucket(5, 4));
		// make sure table will give me a tag and swap
		int swap = 5;
		for (int i = 0; i < 1000; i++) {
			swap = table.swapRandomTagInBucket(5, swap);
		}
		HashSet<Integer> tagVals = new HashSet<>();
		tagVals.add(swap);
		tagVals.add(table.readTag(5, 0));
		tagVals.add(table.readTag(5, 1));
		tagVals.add(table.readTag(5, 2));
		tagVals.add(table.readTag(5, 3));
		assertTrue(tagVals.size() == 5);
		assertTrue(tagVals.contains(1));
		assertTrue(tagVals.contains(2));
		assertTrue(tagVals.contains(3));
		assertTrue(tagVals.contains(4));
		assertTrue(tagVals.contains(5));
	}

	@Test
	public void testBitBleedWithinBucket() {
		int canaryTag = 0b11111111111111111111111111111111;
		FilterTable table = FilterTable.create(12, 1000, 2000000);
		// buckets can hold 4 tags
		table.writeTag(5, 0, canaryTag);
		table.writeTag(5, 2, canaryTag);
		assertTrue(table.readTag(5, 1) == 0);
		assertTrue(table.readTag(5, 3) == 0);
	}

	@Test
	public void testBitBleedBetweenBuckets() {
		int canaryTag = 0b11111111111111111111111111111111;
		FilterTable table = FilterTable.create(12, 1000, 2000000);
		// buckets can hold 4 tags
		table.writeTag(5, 0, canaryTag);
		table.writeTag(5, 3, canaryTag);
		// should be directly adjacent to positions we filled
		assertTrue(table.readTag(4, 3) == 0);
		assertTrue(table.readTag(6, 0) == 0);
	}

	@Test
	public void testEquals() {
		new EqualsTester()
				// we don't test arg2 because numbuckets is only used to create
				// BitSet of estimated proper size.
				.addEqualityGroup(FilterTable.create(12, 1000, 2000000))
				.addEqualityGroup(FilterTable.create(13, 1000, 2000000))
				.addEqualityGroup(FilterTable.create(12, 1000, 3000000)).testEquals();
	}

	@Test
	public void testCopy() {
		FilterTable table = FilterTable.create(12, 1000, 2000000);
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
		SerializableTester.reserializeAndAssert(FilterTable.create(12, 1000, 2000000));
	}

}
