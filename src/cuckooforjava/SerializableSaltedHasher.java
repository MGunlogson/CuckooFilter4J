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

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.ObjectInputStream;
import java.io.Serializable;
import java.security.SecureRandom;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.commons.lang3.SerializationUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

public class SerializableSaltedHasher<T> implements Serializable {
	/**
	 * exists because Guava doesn't setup salt and seed automatically and
	 * because Guavas's HashFunction is NOT serializable
	 */
	private static final long serialVersionUID = 1L;
	private final long seedNSalt;// provides some protection against collision
									// attacks
	private final long addlSipSeed;
	private final Algorithm alg;
	private transient HashFunction hasher;
	private final Funnel<? super T> funnel;

	public enum Algorithm {
		Murmur3_32(0), Murmur3_128(1), sha1(2), sha256(2),sipHash24(3);
		private final int id;

		Algorithm(int id) {
			this.id = id;
		}

		public int getValue() {
			return id;
		}
	}

	public SerializableSaltedHasher(Algorithm alg, Funnel<? super T> funnel) {
		checkNotNull(alg);
		checkNotNull(funnel);
		this.alg = alg;
		this.funnel = funnel;
		SecureRandom randomer =new SecureRandom();
		this.seedNSalt = randomer.nextLong();
		this.addlSipSeed = randomer.nextLong();
		hasher = configureHash();

	}
@VisibleForTesting
	 SerializableSaltedHasher(long seedNSalt,long addlSipSeed, Funnel<? super T> funnel, Algorithm alg) {
		checkNotNull(alg);
		checkNotNull(funnel);
		this.alg = alg;
		this.funnel = funnel;
		this.seedNSalt = seedNSalt;
		this.addlSipSeed = addlSipSeed;
		hasher = configureHash();

	}

	private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
		// default deserialization
		ois.defaultReadObject();
		//not serializable so we rebuild here
		hasher = configureHash();
	}

	private HashFunction configureHash() {
		switch (alg) {
		case Murmur3_32:
			return Hashing.murmur3_32((int) seedNSalt);
		case Murmur3_128:
			return Hashing.murmur3_128((int) seedNSalt);
		case sha1:
			return Hashing.sha1();
		case sha256:
			return Hashing.sha256();
		case sipHash24:
			return Hashing.sipHash24(seedNSalt,addlSipSeed );
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
			return this.seedNSalt == that.seedNSalt && this.alg.equals(that.alg) && this.funnel.equals(that.funnel) && this.addlSipSeed == that.addlSipSeed;
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(seedNSalt, alg, funnel,addlSipSeed);
	}

	public SerializableSaltedHasher<T> copy() {

		return new SerializableSaltedHasher<T>(seedNSalt,addlSipSeed, SerializationUtils.clone(funnel), alg);
	}

}
