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

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.ObjectInputStream;
import java.io.Serializable;
import java.security.SecureRandom;
import java.util.Objects;

import javax.annotation.Nullable;

import com.github.mgunlogson.cuckoofilter4j.Utils.Algorithm;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

/**
 * Serializable, salted wrapper class for Guava's HashFunctions exists because
 * Guava doesn't setup salt and seed automatically and because Guavas's
 * HashFunction is NOT serializable
 * 
 * @author Mark Gunlogson
 *
 * @param <T>
 *            type of item to hash
 */
public class SerializableSaltedHasher<T> implements Serializable {
	/**
	
	 */
	private static final long serialVersionUID = 1L;
	private final long seedNSalt;// provides some protection against collision
									// attacks
	private final long addlSipSeed;
	private final Algorithm alg;
	private transient HashFunction hasher;
	private final Funnel<? super T> funnel;

	@VisibleForTesting
	SerializableSaltedHasher(long seedNSalt, long addlSipSeed, Funnel<? super T> funnel, Algorithm alg) {
		checkNotNull(alg);
		checkNotNull(funnel);
		this.alg = alg;
		this.funnel = funnel;
		this.seedNSalt = seedNSalt;
		this.addlSipSeed = addlSipSeed;
		hasher = configureHash(alg, seedNSalt, addlSipSeed);
	}

	public static <T> SerializableSaltedHasher<T> create(int hashBitsNeeded, Funnel<? super T> funnel) {
		Algorithm alg = Algorithm.Murmur3_32;
		if (hashBitsNeeded > 32)
			alg = Algorithm.Murmur3_128;
		return create(alg, funnel);
	}

	public static <T> SerializableSaltedHasher<T> create(Algorithm alg, Funnel<? super T> funnel) {
		checkNotNull(alg);
		checkNotNull(funnel);
		SecureRandom randomer = new SecureRandom();
		long seedNSalt = randomer.nextLong();
		long addlSipSeed = randomer.nextLong();
		return new SerializableSaltedHasher<>(seedNSalt, addlSipSeed, funnel, alg);
	}

	private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
		// default deserialization
		ois.defaultReadObject();
		// not serializable so we rebuild here
		hasher = configureHash(alg, seedNSalt, addlSipSeed);
	}

	private static HashFunction configureHash(Algorithm alg, long seedNSalt, long addlSipSeed) {
		switch (alg) {
		case Murmur3_32:
			return Hashing.murmur3_32((int) seedNSalt);
		case Murmur3_128:
			return Hashing.murmur3_128((int) seedNSalt);
		case sha256:
			return Hashing.sha1();
		case sipHash24:
			return Hashing.sipHash24(seedNSalt, addlSipSeed);
		default:
			throw new IllegalArgumentException("Invalid Enum Hashing Algorithm???");
		}
	}

	HashCode hashObj(T object) {
		Hasher hashInst = hasher.newHasher();
		hashInst.putObject(object, funnel);
		hashInst.putLong(seedNSalt);
		return hashInst.hash();
	}

	/**
	 * hashes the object with an additional salt. For purpose of the cuckoo
	 * filter, this is used when the hash generated for an item is all zeros.
	 * All zeros is the same as an empty bucket, so obviously it's not a valid
	 * tag. 
	 */
	HashCode hashObjWithSalt(T object, int moreSalt) {
		Hasher hashInst = hasher.newHasher();
		hashInst.putObject(object, funnel);
		hashInst.putLong(seedNSalt);
		hashInst.putInt(moreSalt);
		return hashInst.hash();
	}

	@Override
	public boolean equals(@Nullable Object object) {
		if (object == this) {
			return true;
		}
		if (object instanceof SerializableSaltedHasher) {
			SerializableSaltedHasher<?> that = (SerializableSaltedHasher<?>) object;
			return this.seedNSalt == that.seedNSalt && this.alg.equals(that.alg) && this.funnel.equals(that.funnel)
					&& this.addlSipSeed == that.addlSipSeed;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(seedNSalt, alg, funnel, addlSipSeed);
	}

	int codeBitSize() {
		return hasher.bits();
	}

	public SerializableSaltedHasher<T> copy() {

		return new SerializableSaltedHasher<>(seedNSalt, addlSipSeed, funnel, alg);
	}

}
