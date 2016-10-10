
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

import com.github.mgunlogson.cuckoofilter4j.CuckooFilter;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

public class BenchGuavaBloom {
	public static void main(String[] args) throws Exception {
		ArrayList<Long> cuckooTimes = new ArrayList<>();
		ArrayList<Long> bloomTimes = new ArrayList<>();
		final int filterKeys = 1000000;
		// repeat a bunch of times
		for (int j = 0; j < 100; j++) {

			// create filters
			CuckooFilter<Integer> cuckoo = new CuckooFilter.Builder<>(Funnels.integerFunnel(), filterKeys)
					.withFalsePositiveRate(0.03).build();
			BloomFilter<Integer> bloom = BloomFilter.create(Funnels.integerFunnel(), filterKeys, 0.03);

			// =============CUCKOO=================
			// // warmup inserts
			// for (int i = 110000; i < 115000; i++) {
			// if (!cuckoo.put(i)) {
			// throw new Exception();
			// }
			// }

			long cuckooStart = System.nanoTime();
			// benchmark insert
			for (int i = 0; i < filterKeys * .75; i++) {
				if (!cuckoo.put(i)) {
					throw new Exception();
				}
			}
			// benchmark contains(with half true half false expected)
			for (int i = 0; i < filterKeys; i++) {
				cuckoo.mightContain(i);
			}
			long cuckooEnd = System.nanoTime();

			cuckooTimes.add(cuckooEnd - cuckooStart);
			// ==============BLOOM==============
			// // warmup inserts
			// for (int i = 110000; i < 115000; i++) {
			// bloom.put(i);
			// }

			long bloomStart = System.nanoTime();
			// benchmark insert
			for (int i = 0; i < filterKeys * .75; i++) {
				bloom.put(i);
			}
			// benchmark contains(with half true half false expected)
			for (int i = 0; i < filterKeys; i++) {
				bloom.mightContain(i);
			}
			long bloomEnd = System.nanoTime();
			bloomTimes.add(bloomEnd - bloomStart);
		}
		long cuckooSum = 0;
		for (long time : cuckooTimes) {
			cuckooSum += time;
		}
		long bloomSum = 0;
		for (long time : bloomTimes) {
			bloomSum += time;
		}
		System.out.println("Average Times (seconds)--- Bloom: " + bloomSum / 1000000000.0 + "   Cuckoo: "
				+ cuckooSum / 1000000000.0);
	}
}
