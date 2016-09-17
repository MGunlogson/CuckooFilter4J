
import java.util.ArrayList;

import com.cuckooforjava.CuckooFilter;
import com.google.common.hash.Funnels;

public class BenchCuckooForProfiling {
	public static void main(String[] args) throws Exception {

		for (int j = 0; j < 10000; j++) {

			// create filters
			CuckooFilter<Integer> cuckoo = CuckooFilter.create(Funnels.integerFunnel(), 100000, 0.03);
			// benchmark insert
			for (int i = 0; i < 70000; i++) {
				if (!cuckoo.put(i)) {
					throw new Exception();
				}
			}
			// benchmark contains(with half true half false expected)
			for (int i = 35000; i < 105000; i++) {
				cuckoo.mightContain(i);
			}

		}
	}

}
