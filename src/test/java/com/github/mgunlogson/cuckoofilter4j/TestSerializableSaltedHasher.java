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

import java.util.ArrayList;
import java.util.HashSet;

import org.junit.Test;

import com.github.mgunlogson.cuckoofilter4j.SerializableSaltedHasher;
import com.github.mgunlogson.cuckoofilter4j.Utils.Algorithm;
import com.google.common.hash.Funnels;
import com.google.common.testing.ClassSanityTester;
import com.google.common.testing.EqualsTester;
import com.google.common.testing.SerializableTester;

public class TestSerializableSaltedHasher {

	@Test(expected = NullPointerException.class)
	public void testConsturctorNullArgs() {
		SerializableSaltedHasher.create(null, null);
	}

	@Test(expected = NullPointerException.class)
	public void testConsturctorNullArgs2() {
		SerializableSaltedHasher.create(null, Funnels.integerFunnel());
	}

	@Test
	public void testSeeds() {
		// test a seeded hash alg
		SerializableSaltedHasher<Integer> hasher1 = new SerializableSaltedHasher<>(1, 0, Funnels.integerFunnel(),
				Algorithm.Murmur3_32);
		SerializableSaltedHasher<Integer> hasher2 = new SerializableSaltedHasher<>(2, 0, Funnels.integerFunnel(),
				Algorithm.Murmur3_32);
		assertFalse(hasher2.hashObj(42).equals(hasher1.hashObj(42)));
		assertFalse(hasher2.hashObjWithSalt(42, 1).equals(hasher1.hashObjWithSalt(42, 1)));
		assertFalse(hasher2.hashObjWithSalt(42, 1).equals(hasher1.hashObj(42)));
		// test salted alg
		hasher1 = new SerializableSaltedHasher<>(1, 0, Funnels.integerFunnel(), Algorithm.sha256);
		hasher2 = new SerializableSaltedHasher<>(2, 0, Funnels.integerFunnel(), Algorithm.sha256);
		assertFalse(hasher2.hashObj(42).equals(hasher1.hashObj(42)));
		assertFalse(hasher2.hashObjWithSalt(42, 1).equals(hasher1.hashObjWithSalt(42, 1)));
		assertFalse(hasher2.hashObjWithSalt(42, 1).equals(hasher1.hashObj(42)));

		// test seeded-salted algs like SIPHash
		ArrayList<SerializableSaltedHasher<Integer>> hashAry = new ArrayList<SerializableSaltedHasher<Integer>>();
		hashAry.add(new SerializableSaltedHasher<Integer>(1, 1, Funnels.integerFunnel(), Algorithm.sipHash24));
		hashAry.add(new SerializableSaltedHasher<Integer>(2, 1, Funnels.integerFunnel(), Algorithm.sipHash24));
		hashAry.add(new SerializableSaltedHasher<Integer>(1, 2, Funnels.integerFunnel(), Algorithm.sipHash24));
		hashAry.add(new SerializableSaltedHasher<Integer>(2, 2, Funnels.integerFunnel(), Algorithm.sipHash24));
		HashSet<Long> results = new HashSet<>();
		for (SerializableSaltedHasher<Integer> hashVal : hashAry) {
			long unsalted = hashVal.hashObj(42).asLong();
			assertFalse(results.contains(unsalted));
			results.add(unsalted);
			long salty = hashVal.hashObjWithSalt(42, 1).asLong();
			assertFalse(results.contains(salty));
			results.add(salty);
		}
	}

	@Test
	public void testAutoAlgorithm() {
		SerializableSaltedHasher<Integer> hasher = SerializableSaltedHasher.create(100, Funnels.integerFunnel());
		assertTrue(hasher.codeBitSize() == 128);
		hasher = SerializableSaltedHasher.create(30, Funnels.integerFunnel());
		assertTrue(hasher.codeBitSize() < 128);
	}

	@Test
	public void testEquals() {
		new EqualsTester()
				.addEqualityGroup(
						new SerializableSaltedHasher<byte[]>(0, 0, Funnels.byteArrayFunnel(), Algorithm.Murmur3_32))
				.addEqualityGroup(
						new SerializableSaltedHasher<byte[]>(1, 0, Funnels.byteArrayFunnel(), Algorithm.Murmur3_32))
				.addEqualityGroup(
						new SerializableSaltedHasher<byte[]>(0, 1, Funnels.byteArrayFunnel(), Algorithm.Murmur3_32))
				.addEqualityGroup(
						new SerializableSaltedHasher<Integer>(0, 0, Funnels.integerFunnel(), Algorithm.Murmur3_32))
				.addEqualityGroup(
						new SerializableSaltedHasher<byte[]>(0, 0, Funnels.byteArrayFunnel(), Algorithm.sha256))
				.testEquals();
	}

	@Test
	public void testEqualsSame() {
		assertTrue(new SerializableSaltedHasher<Integer>(0, 0, Funnels.integerFunnel(), Algorithm.Murmur3_32)
				.equals(new SerializableSaltedHasher<Integer>(0, 0, Funnels.integerFunnel(), Algorithm.Murmur3_32)));
	}

	@Test
	public void testCopy() {
		SerializableSaltedHasher<Integer> hasher = new SerializableSaltedHasher<>(0, 0, Funnels.integerFunnel(),
				Algorithm.Murmur3_32);
		SerializableSaltedHasher<Integer> hasherCopy = hasher.copy();
		assertTrue(hasherCopy.equals(hasher));
		assertNotSame(hasher, hasherCopy);
	}

	@Test
	public void autoTestNulls() {
		new ClassSanityTester().testNulls(SerializableSaltedHasher.class);
	}

	@Test
	public void testSerialize() {
		SerializableTester.reserializeAndAssert(
				new SerializableSaltedHasher<Integer>(0, 0, Funnels.integerFunnel(), Algorithm.Murmur3_32));
	}

}
