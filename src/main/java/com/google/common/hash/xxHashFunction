package com.google.common.hash;

import java.io.Serializable;

public class xxHashFunction extends AbstractStreamingHashFunction implements Serializable {

	private static final long serialVersionUID = -3736964476904747967L;
	private final long seed;
	
	public xxHashFunction(long newSeed){
		seed = newSeed;
	}

	@Override
	public Hasher newHasher() {
		return new xxHasher(seed);
	}

	@Override
	public int bits() {
		return 64;
	}
	
	static final class xxHasher extends AbstractByteHasher {
		
		private static final long PRIME64_1 = -7046029288634856825L; 
		private static final long PRIME64_2 = -4417276706812531889L; 
		private static final long PRIME64_3 = 1609587929392839161L;
		private static final long PRIME64_4 = -8796714831421723037L; 
		private static final long PRIME64_5 = 2870177450012600261L;
		private final long seed;
		
		private byte[] ba;
		private int baIndex=0;
		
		xxHasher(long newSeed) {
			seed = newSeed;
			ba = new byte[16];
		}

		@Override
		public HashCode hash() {
			return HashCode.fromLong(hash(ba,0,baIndex,seed));
		}

		@Override
		protected void update(byte b) {
			if(baIndex == ba.length) expand();
			ba[baIndex++] = b;
		}
		
	  @Override
	  public Hasher putInt(int value) {
		if(baIndex+3 >=ba.length) expand();
	    ba[baIndex+3] = (byte)(value >>> 24);
	    ba[baIndex+2] = (byte)(value >>> 16);
	    ba[baIndex+1] = (byte)(value >>> 8);
	    ba[baIndex] =(byte)value;
	    baIndex+=4;
	    return this;
	  }

	  @Override
	  public Hasher putLong(long value) {
		if(baIndex+7 >=ba.length) expand();
	  	ba[baIndex+7] =  (byte)(value >>> 56);
	  	ba[baIndex+6] =  (byte)(value >>> 48);
	  	ba[baIndex+5] =  (byte)(value >>> 40);
	  	ba[baIndex+4] =  (byte)(value >>> 32);
	  	ba[baIndex+3] =  (byte)(value >>> 24);
	  	ba[baIndex+2] =  (byte)(value >>> 16);
	  	ba[baIndex+1] =  (byte)(value >>> 8);
	  	ba[baIndex] =  (byte)value;
	  	baIndex+=8;
	    return this;
	  }
	  
	  private void expand() {
		  	byte[] newBa = new byte[ba.length*2];
			for(int i=ba.length-1; i>=0; i--) newBa[i] = ba[i];
			baIndex = ba.length;
			ba = newBa;
	  }
		
		private static long readLongLE(byte[] buf, int i) {
	        return (buf[i] & 0xFFL) | ((buf[i+1] & 0xFFL) << 8) | ((buf[i+2] & 0xFFL) << 16) | ((buf[i+3] & 0xFFL) << 24)
	                | ((buf[i+4] & 0xFFL) << 32) | ((buf[i+5] & 0xFFL) << 40) | ((buf[i+6] & 0xFFL) << 48) | ((buf[i+7] & 0xFFL) << 56);
	    }
		
		private static int readIntLE(byte[] buf, int i) {
		        return (buf[i] & 0xFF) | ((buf[i+1] & 0xFF) << 8) | ((buf[i+2] & 0xFF) << 16) | ((buf[i+3] & 0xFF) << 24);
		}
	    
		
		 /**
	     * <p>
	     * Calculates XXHash64 from given {@code byte[]} buffer.
	     * </p><p>
	     * This code comes from <a href="https://github.com/jpountz/lz4-java">LZ4-Java</a> created
	     * by Adrien Grand.
	     * </p>
	     *
	     * @param buf to calculate hash from
	     * @param off offset to start calculation from
	     * @param len length of data to calculate hash
	     * @param seed  hash seed
	     * @return XXHash.
	     */
	    private static long hash(byte[] buf, int off, int len, long seed) {
	        if (len < 0) {
	            throw new IllegalArgumentException("lengths must be >= 0");
	        }
	        if(off<0 || off>=buf.length || off+len<0 || off+len>buf.length){
	            throw new IndexOutOfBoundsException();
	        }

	        final int end = off + len;
	        long h64;

	        if (len >= 32) {
	            final int limit = end - 32;
	            long v1 = seed + PRIME64_1 + PRIME64_2;
	            long v2 = seed + PRIME64_2;
	            long v3 = seed + 0;
	            long v4 = seed - PRIME64_1;
	            do {
	                v1 += readLongLE(buf, off) * PRIME64_2;
	                v1 = Long.rotateLeft(v1, 31);
	                v1 *= PRIME64_1;
	                off += 8;

	                v2 += readLongLE(buf, off) * PRIME64_2;
	                v2 = Long.rotateLeft(v2, 31);
	                v2 *= PRIME64_1;
	                off += 8;

	                v3 += readLongLE(buf, off) * PRIME64_2;
	                v3 = Long.rotateLeft(v3, 31);
	                v3 *= PRIME64_1;
	                off += 8;

	                v4 += readLongLE(buf, off) * PRIME64_2;
	                v4 = Long.rotateLeft(v4, 31);
	                v4 *= PRIME64_1;
	                off += 8;
	            } while (off <= limit);

	            h64 = Long.rotateLeft(v1, 1) + Long.rotateLeft(v2, 7) + Long.rotateLeft(v3, 12) + Long.rotateLeft(v4, 18);

	            v1 *= PRIME64_2; v1 = Long.rotateLeft(v1, 31); v1 *= PRIME64_1; h64 ^= v1;
	            h64 = h64 * PRIME64_1 + PRIME64_4;

	            v2 *= PRIME64_2; v2 = Long.rotateLeft(v2, 31); v2 *= PRIME64_1; h64 ^= v2;
	            h64 = h64 * PRIME64_1 + PRIME64_4;

	            v3 *= PRIME64_2; v3 = Long.rotateLeft(v3, 31); v3 *= PRIME64_1; h64 ^= v3;
	            h64 = h64 * PRIME64_1 + PRIME64_4;

	            v4 *= PRIME64_2; v4 = Long.rotateLeft(v4, 31); v4 *= PRIME64_1; h64 ^= v4;
	            h64 = h64 * PRIME64_1 + PRIME64_4;
	        } else {
	            h64 = seed + PRIME64_5;
	        }

	        h64 += len;

	        while (off <= end - 8) {
	            long k1 = readLongLE(buf, off);
	            k1 *= PRIME64_2; k1 = Long.rotateLeft(k1, 31); k1 *= PRIME64_1; h64 ^= k1;
	            h64 = Long.rotateLeft(h64, 27) * PRIME64_1 + PRIME64_4;
	            off += 8;
	        }

	        if (off <= end - 4) {
	            h64 ^= (readIntLE(buf, off) & 0xFFFFFFFFL) * PRIME64_1;
	            h64 = Long.rotateLeft(h64, 23) * PRIME64_2 + PRIME64_3;
	            off += 4;
	        }

	        while (off < end) {
	            h64 ^= (buf[off] & 0xFF) * PRIME64_5;
	            h64 = Long.rotateLeft(h64, 11) * PRIME64_1;
	            ++off;
	        }

	        h64 ^= h64 >>> 33;
	        h64 *= PRIME64_2;
	        h64 ^= h64 >>> 29;
	        h64 *= PRIME64_3;
	        h64 ^= h64 >>> 32;

	        return h64;
	    }
		
	}

}
