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

import java.util.ArrayList;

import com.cuckooforjava.CuckooFilter;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

public class BenchGuavaBloomInsertSpeed {
	public static void main(String[] args) throws Exception {
		long cuckooTime;
		long bloomTime;
		int filterSize=100000000;
			// create filters
			CuckooFilter<Integer> cuckoo = CuckooFilter.create(Funnels.integerFunnel(), filterSize, 0.03);
			BloomFilter<Integer> bloom = BloomFilter.create(Funnels.integerFunnel(), filterSize, 0.03);

			// =============CUCKOO=================
			// warmup inserts
			for (int i = filterSize-(filterSize/10); i < filterSize; i++) {
				if (!cuckoo.put(i)) {
					throw new Exception();
				}
			}

			long cuckooStart = System.nanoTime();
			// benchmark insert
			for (int i = 0; i < filterSize*.75; i++) {
				if (!cuckoo.put(i)) {
					throw new Exception();
				}
			}
			long cuckooEnd = System.nanoTime();

			cuckooTime=cuckooEnd - cuckooStart;
			// ==============BLOOM==============
			// warmup inserts
			for (int i = filterSize-(filterSize/10); i < filterSize; i++) {
				bloom.put(i);
			}

			long bloomStart = System.nanoTime();
			// benchmark insert
			for (int i = 0; i < filterSize*.75; i++) {
				bloom.put(i);
			}
			long bloomEnd = System.nanoTime();
			bloomTime=bloomEnd - bloomStart;
			

		System.out.println("Average Inserts Per Second, Bloom: " +  (filterSize*.75)/(bloomTime /1000000000.0)  + "   Cuckoo: "
				+  (filterSize*.75)/(cuckooTime / 1000000000.0));
	}
}
