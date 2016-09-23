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
   limitations under the License.TWARE.
*/

package com.cuckooforjava;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Random;

import org.junit.Test;

import com.cuckooforjava.CuckooFilter.Algorithm;
import com.google.common.hash.Funnels;
import com.google.common.testing.ClassSanityTester;
import com.google.common.testing.EqualsTester;
import com.google.common.testing.SerializableTester;

public class TestIndexTagCalc {

	@Test(expected = IllegalArgumentException.class)
	public void filterTooBig() {
		IndexTagCalc.create(Algorithm.Murmur3_32, Funnels.integerFunnel(), Long.MAX_VALUE, 1);
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void filterTooBig2() {
		IndexTagCalc.create(Algorithm.Murmur3_32, Funnels.integerFunnel(), 30000, Integer.MAX_VALUE);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInvalidArgs() {
		IndexTagCalc.create(Funnels.integerFunnel(), 0, 1);
	}
	@Test(expected = IllegalArgumentException.class)
	public void testInvalidArgs2() {
		IndexTagCalc.create( Funnels.integerFunnel(), 1, 0);
	}


	@Test
	public void knownZeroTags32() {
		// manual instantiation to force salts to be static
		SerializableSaltedHasher<Integer> hasher = new SerializableSaltedHasher<>(0, 0, Funnels.integerFunnel(),
				Algorithm.Murmur3_32);
		IndexTagCalc<Integer> indexer = new IndexTagCalc<>(hasher, 128, 4);
		// find some 0 value tags
		int zeroTags = 0;
		int i = 0;
		ArrayList<Integer> zeroTagInputs = new ArrayList<>();
		while (zeroTags < 20) {
			if (indexer.getTagValue32(hasher.hashObj(i).asInt()) == 0) {
				zeroTagInputs.add(i);
				zeroTags++;
			}
			i++;
		}
		for (Integer tag : zeroTagInputs) {
			// none of the zero value tags should be zero from indexer if
			// rehashing is working properly
			assertFalse(indexer.generate(tag).tag == 0);
		}

	}
	

	@Test
	public void sanityTagIndexBitsUsed32() {
		// manual instantiation to force salts to be static
		SerializableSaltedHasher<Integer> hasher = new SerializableSaltedHasher<>(0, 0, Funnels.integerFunnel(),
				Algorithm.Murmur3_32);
		IndexTagCalc<Integer> indexer = new IndexTagCalc<>(hasher, 128, 4);
		long setBitsIndex = 0;
		long setBitsTag = 0;
		// should be enough to set all bits being used...
		for (int i = 0; i < 12345; i++) {
			BucketAndTag bt = indexer.generate(i);
			setBitsIndex |= bt.index;
			setBitsTag |= bt.tag;
		}
		// will be true if we're using the right number of bits for tag and
		// index for this calculator
		assertTrue(Long.bitCount(setBitsIndex) == 7);
		assertTrue(Long.bitCount(setBitsTag) == 4);
		// check where the set bits are
		long indexMask = 0b1111111;
		long tagMask =   0b0001111;
		assertTrue(indexMask == setBitsIndex);
		assertTrue(tagMask == setBitsTag);
	}
	
	@Test
	public void sanityTagIndexBitsUsed64() {
		// manual instantiation to force salts to be static
		SerializableSaltedHasher<Integer> hasher = new SerializableSaltedHasher<>(0, 0, Funnels.integerFunnel(),
				Algorithm.sipHash24);
		IndexTagCalc<Integer> indexer = new IndexTagCalc<>(hasher,(long)Math.pow(2, 31) , 32);
		long setBitsIndex = 0;
		long setBitsTag = 0;
		// should be enough to set all bits being used...
		for (int i = 0; i < 1234567; i++) {
			BucketAndTag bt = indexer.generate(i);
			setBitsIndex |= bt.index;
			setBitsTag |= bt.tag;
		}
		// will be true if we're using the right number of bits for tag and
		// index for this calculator
		assertTrue(Long.bitCount(setBitsIndex) == 31);
		assertTrue(Long.bitCount(setBitsTag) == 32);
		// check where the set bits are
		long bitMask32 = -1L>>>32;//(mask for lower 32 bits set)
		long bitMask31 = bitMask32>>>1;//(mask for lower 32 bits set)
		assertTrue(bitMask32 == setBitsTag);
		assertTrue(bitMask31 == setBitsIndex);
	}
	
	@Test
	public void sanityTagIndexBitsUsed128() {
		// manual instantiation to force salts to be static
		SerializableSaltedHasher<Integer> hasher = new SerializableSaltedHasher<>(0, 0, Funnels.integerFunnel(),
				Algorithm.sha256);
		IndexTagCalc<Integer> indexer = new IndexTagCalc<>(hasher,(long)Math.pow(2, 62) , 64);
		long setBitsIndex = 0;
		long setBitsTag = 0;
		// should be enough to set all bits being used...
		for (int i = 0; i < 1234567; i++) {
			BucketAndTag bt = indexer.generate(i);
			setBitsIndex |= bt.index;
			setBitsTag |= bt.tag;
		}
		// will be true if we're using the right number of bits for tag and
		// index for this calculator
		assertTrue(Long.bitCount(setBitsIndex) == 64);
		assertTrue(Long.bitCount(setBitsTag) == 64);
		// check where the set bits are
		long bitMask = -1L;//(mask for all 64 bits set)
		assertTrue(bitMask == setBitsIndex);
		assertTrue(bitMask == setBitsTag);
	}
	
	
	@Test
	public void sanityTagIndexNotSame() {
		// manual instantiation to force salts to be static
		SerializableSaltedHasher<Integer> hasher = new SerializableSaltedHasher<>(0, 0, Funnels.integerFunnel(),
				Algorithm.sha256);
		IndexTagCalc<Integer> indexer = new IndexTagCalc<>(hasher, (long)Math.pow(2,62), 64);
		// should be enough to set all bits being used...
		for (int i = 0; i < 1234567; i+=4) {
			BucketAndTag bt = indexer.generate(i);
			BucketAndTag bt2 = indexer.generate(i+1);
			//we use two equalities to make collisions super-rare since we otherwise only have 32 bits of hash to compare
			//we're checking for 2 collisions in 2 pairs of 32 bit hash. Should be as hard as getting a single 64 bit collision aka... never happen
			assertTrue(bt.index != bt.tag || bt2.index != bt2.tag  );
		}
	}
	
	
	

	@Test
	public void testEquals() {
		new EqualsTester().addEqualityGroup(new IndexTagCalc<Integer>(getUnsaltedHasher(), 128, 4))
				.addEqualityGroup(new IndexTagCalc<Integer>(getUnsaltedHasher(), 256, 4))
				.addEqualityGroup(new IndexTagCalc<Integer>(getUnsaltedHasher(), 128, 6)).testEquals();
	}

	@Test
	public void testEqualsSame() {
		assertTrue(new IndexTagCalc<Integer>(getUnsaltedHasher(), 128, 4)
				.equals(new IndexTagCalc<Integer>(getUnsaltedHasher(), 128, 4)));
	}

	@Test
	public void testCopy() {
		IndexTagCalc<Integer> calc = IndexTagCalc.create(Funnels.integerFunnel(), 128, 4);
		IndexTagCalc<Integer> calcCopy = calc.copy();
		assertTrue(calcCopy.equals(calc));
		assertNotSame(calc, calcCopy);
	}

	@Test
	public void autoTestNulls() {
		// chose 8 for int so it passes bucket and tag size checks
		new ClassSanityTester().setDefault(SerializableSaltedHasher.class, getUnsaltedHasher()).setDefault(long.class, 8L).setDefault(int.class, 8)
				.testNulls(IndexTagCalc.class);
	}

	@Test
	public void brokenAltIndex32() {
		Random rando = new Random();
		IndexTagCalc<Integer> calc = IndexTagCalc.create( Funnels.integerFunnel(), 2048, 14);
		for (int i = 0; i < 10000; i++) {
			BucketAndTag pos = calc.generate(rando.nextInt());
			long altIndex = calc.altIndex(pos.index, pos.tag);
			assertTrue(pos.index == calc.altIndex(altIndex, pos.tag));
		}
	}
	@Test
	public void brokenAltIndex64() {
		Random rando = new Random();
		IndexTagCalc<Integer> calc = IndexTagCalc.create( Funnels.integerFunnel(), (long)Math.pow(2,32), 5);
		for (int i = 0; i < 10000; i++) {
			BucketAndTag pos = calc.generate(rando.nextInt());
			long altIndex = calc.altIndex(pos.index, pos.tag);
			assertTrue(pos.index == calc.altIndex(altIndex, pos.tag));
		}
	}

	@Test
	public void testSerialize() {
		SerializableTester.reserializeAndAssert(new IndexTagCalc<Integer>(getUnsaltedHasher(), 128, 4));
	}

	// using this because otherwise internal state of salts in hasher will
	// prevent equality comparisons
	private SerializableSaltedHasher<Integer> getUnsaltedHasher() {
		return new SerializableSaltedHasher<>(0, 0, Funnels.integerFunnel(), Algorithm.Murmur3_32);
	}

}
