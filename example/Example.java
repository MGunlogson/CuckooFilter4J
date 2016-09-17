import com.cuckooforjava.CuckooFilter;
import com.google.common.hash.Funnels;

public class Example {

	public static void main(String[] args) {
		// create
		CuckooFilter<Integer> filter = CuckooFilter.create(Funnels.integerFunnel(), 100000);
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
