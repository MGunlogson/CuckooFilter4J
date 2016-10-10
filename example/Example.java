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

import com.github.mgunlogson.cuckoofilter4j.CuckooFilter;
import com.google.common.hash.Funnels;

public class Example {

	public static void main(String[] args) {
		// create
		CuckooFilter<Integer> filter = new CuckooFilter.Builder<>(Funnels.integerFunnel(), 100000).build();
		// insert
		if (filter.put(42)) {
			System.out.println("Insert Success!");
		}
		// contains
		if (filter.mightContain(42)) {
			System.out.println("Found 42!");
		}
		// count
		System.out.println("Filter has " + filter.getCount() + " items");

		// % loaded
		System.out.println("Filter is " + String.format("%.0f%%", filter.getLoadFactor() * 100) + " loaded");

		// delete
		if (filter.delete(42)) {
			System.out.println("Delete Success!");
		}
	}

}
