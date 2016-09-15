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

import java.util.ArrayList;
import java.util.HashSet;

import org.junit.Test;

import com.google.common.hash.Funnels;
import com.google.common.testing.ClassSanityTester;
import com.google.common.testing.EqualsTester;
import com.google.common.testing.SerializableTester;

import cuckooforjava.SerializableSaltedHasher.Algorithm;

public class TestSerializableSaltedHasher {

	@Test(expected = NullPointerException.class)
	public void testConsturctorNullArgs() {
		new SerializableSaltedHasher<Object>(Algorithm.Murmur3_32, null);
	}

	@Test(expected = NullPointerException.class)
	public void testConsturctorNullArgs2() {
		new SerializableSaltedHasher<Object>(null, TestUtils.BAD_FUNNEL);
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
		hasher1 = new SerializableSaltedHasher<>(1, 0, Funnels.integerFunnel(), Algorithm.sha1);
		hasher2 = new SerializableSaltedHasher<>(2, 0, Funnels.integerFunnel(), Algorithm.sha1);
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
				.addEqualityGroup(new SerializableSaltedHasher<byte[]>(0, 0, Funnels.byteArrayFunnel(), Algorithm.sha1))
				.testEquals();
	}

	@Test
	public void testEqualsSame() {
		assertTrue(new SerializableSaltedHasher<Integer>(0, 0, Funnels.integerFunnel(), Algorithm.Murmur3_32)
				.equals(new SerializableSaltedHasher<Integer>(0, 0, Funnels.integerFunnel(), Algorithm.Murmur3_32)));
	}

	@Test
	public void testEqualsWithCustomFunnel() {
		SerializableSaltedHasher<Integer> h1 = new SerializableSaltedHasher<>(0, 0, new FakeFunnel(),
				Algorithm.Murmur3_32);
		SerializableSaltedHasher<Integer> h2 = new SerializableSaltedHasher<>(0, 0, new FakeFunnel(),
				Algorithm.Murmur3_32);
		assertEquals(h1, h2);
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
