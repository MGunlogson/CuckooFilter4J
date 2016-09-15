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

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public final class TestUtils {
	private TestUtils() {

	}

	static final Funnel<Object> BAD_FUNNEL = new Funnel<Object>() {
		private static final long serialVersionUID = 1L;

		public void funnel(Object object, PrimitiveSink bytePrimitiveSink) {
			bytePrimitiveSink.putInt(object.hashCode());
		}
	};

}

final class FakeFunnel implements Funnel<Integer> {
	private static final long serialVersionUID = 1L;

	@Override
	public void funnel(Integer value, PrimitiveSink into) {
		into.putInt(value);
	}

	@Override
	public boolean equals(Object object) {
		return object instanceof FakeFunnel;
	}

	@Override
	public int hashCode() {
		return 42;
	}
}
