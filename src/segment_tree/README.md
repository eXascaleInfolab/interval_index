segment_tree
============

Canonic implementation of Segment tree from Wikipedia
http://en.wikipedia.org/wiki/Segment_tree

HOWTO (also in 'example.cpp')

{
	
	/*
	 *  1. create tree with intervals: [0, 3), [1, 5), [3, 10), [8, 20)
	 *  attach their unique ids as values respectively:  1, 2, 3, 4
	 */

	typedef TSegmentTree<int, int>::TKeyValue TKeyValue;
	vector<TKeyValue> data = { TKeyValue(0, 3, 1),
				   TKeyValue(1, 5, 2),
				   TKeyValue(3, 10, 3),
				   TKeyValue(8, 20, 4) };
	TSegmentTree<int, int> tree(data);


	// 2. ids of intervals which overlap point 3
	// answer: 2, 3
	{
		vector<int> results = tree.Search(3);
		std::for_each(results.begin(), results.end(), print);
		std::cout << "\n";
	}


	// 3. count of intervals which overlap point 3
	// NB: this method doesn't collect results, so it is more efficient
	// answer: 2
	{
		std::cout << tree.Count(3) << "\n";
	}

	// 4. ids of intervals which overlap interval [5, 8)
	// answer: 3, 4
	{
		vector<int> results = tree.Search(5, 8);
		std::for_each(results.begin(), results.end(), print);
		std::cout << "\n";
	}

	// 5. general approach for search, provide a callback class/function
	{
		tree.Search(19, print);
		std::cout << "\n";

		TPrinter printer;
		tree.Search(19, &printer);
		std::cout << "\n";
		tree.Search(12, 20, &printer);
		std::cout << "\n";

		/*
		 * NB: in the following call certain ids will be returned several times.
		 * It happens only if the query is an interval (not a point).
		 * The reason is that segment tree splits original intervals and keep them in several nodes.
		 * For the efficiency in the generic search method there is no cuts of repeating results.
		 * However, such pruning exists in the method "vector<TValue> Search(const TKey& start, const TKey& end) const"
		 */
		tree.Search(-20, 20, &printer);
		std::cout << "\n";

		tree.Search(20, 21, &printer);
		std::cout << "\n";
	}
}
