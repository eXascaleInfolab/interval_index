#ifndef __SEGMENTTREE_H
#define __SEGMENTTREE_H


#include <vector>
#include <set>
#include <algorithm>

using std::vector;
using std::set;


//counts number of results without repeating
//doesn't create a time overhead during query,
//though needs an O(ResponseSize) time to refresh its counters after the query.
class TSegmentTreeBitmapCounter {
public:
	TSegmentTreeBitmapCounter(): ResultsBitMap(1000000, false), Count(0) {
		Results.reserve(10000);
	}
	inline void operator()(const int& value) {
		if (value >= ResultsBitMap.size()) {
			ResultsBitMap.resize((value << 1), false);
		}
		if (!ResultsBitMap[value]) {
			ResultsBitMap[value] = true;
			Results.push_back(value);
		}
	}
	inline long long GetCount() const {
		return Results.size();
	}
	inline void Refresh() {
		for (int index = 0; index < Results.size(); ++index) {
			ResultsBitMap[Results[index]] = false;
		}
		Results.resize(0);
	}
	std::vector<int> Results;
	std::vector<bool> ResultsBitMap;
private:
	long long Count;

};


template <class TKey, class TValue>
class TSegmentTree {
public:
	struct TKeyValue {
		TKey Start, End;
		TValue Value;
		TKeyValue(const TKey& start, const TKey& end, const TKey& value) :
			Start(start),
			End(end),
			Value(value) {
		}
	};

	TSegmentTree (const vector<TKeyValue>& data) {
		BuildTree(data);
	}

	//returns values of intervals which overlap the point
	vector<TValue> Search(const TKey& point) const {
		TCollectorVector collector;
		FindContainingIntervals(point, TREE_ROOT, &collector);
		return collector.Results;
	}

	//returns number of intervals which overlap the point
	//NB: faster than the previous method, doesn't collect values
	int Count(const TKey& point) const {
		TCounter counter;
		FindContainingIntervals(point, TREE_ROOT, &counter);
		return counter.Count;
	}

	//returns (!!!)distinct values of intervals which overlap the interval [start, end)
	vector<TValue> Search(const TKey& start, const TKey& end) const {
		TCollectorSet collector;
		FindOverlappingIntervals(start, end, TREE_ROOT, &collector);
		return vector<TValue>(collector.Results.begin(), collector.Results.end());
	}

	//generic search methods,
	//1. all intervals, which overlap the point
	// calls TCallback(const TValue&) for each found result
	template <class TCallback>
	void Search(const TKey& point, TCallback* callbackPtr) const {
		const TKey& minValue = Tree[TREE_ROOT].Start;
		const TKey& maxValue = Tree[TREE_ROOT].End;
		if (point < minValue || point >= maxValue) {
			return;
		}
		FindContainingIntervals(point, TREE_ROOT, callbackPtr);
	}

	//2. all intervals, which overlap the interval [start, end)
	// calls TCallback(const TValue&) for each found result
	//NB: in this method callback will be called for the same interval more than one time
	template <class TCallback>
	void Search(const TKey& start, const TKey& end, TCallback* callbackPtr) const {
		const TKey& minValue = Tree[TREE_ROOT].Start;
		const TKey& maxValue = Tree[TREE_ROOT].End;
		if (end <= minValue || start >= maxValue) {
			return;
		}
		FindOverlappingIntervals(start, end, TREE_ROOT, callbackPtr);
	}

private:
	struct TNode {
		TKey Start, End;
		vector<TValue> Values;
		TNode(const TKey& start, const TKey& end) :
				Start(start),
				End(end) {
		}
		TNode() {
		}
	};
	static const int TREE_ROOT = 1;
	vector<TNode> Tree;

	void BuildTree(const vector<TKeyValue>& data) {
		vector<TKey> borders;
		for (int intervalIndex = 0; intervalIndex < data.size(); ++intervalIndex) {
			borders.push_back(data[intervalIndex].Start);
			borders.push_back(data[intervalIndex].End);
		}
		std::sort(borders.begin(), borders.end());
		const TKey maxValue = *borders.rbegin();
		vector<TNode> leafNodes;
		for (int nodeIndex = 0; nodeIndex < borders.size(); ++nodeIndex) {
			if (nodeIndex > 0 && borders[nodeIndex] == borders[nodeIndex - 1]) {
				continue;
			}
			const TKey& start = borders[nodeIndex];
			if (leafNodes.size()) {
				leafNodes.rbegin()->End = start;
			}
			leafNodes.push_back(TNode(start, start));
		}
		{//create tree structure
			int insertBefore = 1;
			int layerSize = 1;
			while (layerSize < leafNodes.size()) {
				insertBefore += layerSize;
				layerSize = layerSize << 1;
			}
			Tree = vector<TNode>(insertBefore, TNode(maxValue, maxValue));
			Tree.insert(Tree.end(), leafNodes.begin(), leafNodes.end());
		}
		//fill borders
		for (int position = Tree.size() - leafNodes.size() - 1; position >= TREE_ROOT; --position) {
			const int left_child = position << 1;
			const int right_child = (position << 1) + 1;
			if (left_child < Tree.size()) {
				Tree[position].Start = Tree[left_child].Start;
				Tree[position].End = Tree[left_child].End;
			}
			if (right_child < Tree.size()) {
				Tree[position].End = Tree[right_child].End;
			}
		}
		//fill values
		for (int intervalIndex = 0; intervalIndex < data.size(); ++intervalIndex) {
			const TKey& start = data[intervalIndex].Start;
			const TKey& end = data[intervalIndex].End;
			const TValue& value = data[intervalIndex].Value;
			TSegmentTree::PutInterval(start, end, value, TREE_ROOT);
		}
	}

	//NB: callback will be called for the same value more than one time
	template <class TCallback>
	void FindOverlappingIntervals(const TKey& start, const TKey& end,
								  const int startNode, TCallback* callbackPtr) const {

		if (!Tree[startNode].Values.empty()) {
			for (typename vector<TValue>::const_iterator valueIt = Tree[startNode].Values.begin();
					valueIt != Tree[startNode].Values.end(); ++valueIt) {
				(*callbackPtr)(*valueIt);
			}
		}
		{
			const int leftChild = startNode << 1;
			if (leftChild < Tree.size() && Tree[leftChild].Start <= end && Tree[leftChild].End > start) {
				FindOverlappingIntervals(start, end, leftChild, callbackPtr);
			}
		}
		{
			const int rightChild = (startNode << 1) + 1;
			if (rightChild < Tree.size() && Tree[rightChild].Start <= end && Tree[rightChild].End > start) {
				FindOverlappingIntervals(start, end, rightChild, callbackPtr);
			}
		}
	}
	template <class TCallback>
	void FindContainingIntervals(const TKey& point, const int startNode, TCallback* callbackPtr) const {
		if (!Tree[startNode].Values.empty()) {
			for (typename vector<TValue>::const_iterator valueIt = Tree[startNode].Values.begin();
					valueIt != Tree[startNode].Values.end(); ++valueIt) {
				(*callbackPtr)(*valueIt);
			}
		}
		{
			const int leftChild = startNode << 1;
			if (leftChild < Tree.size() && Tree[leftChild].Start <= point && Tree[leftChild].End > point) {
				FindContainingIntervals(point, leftChild, callbackPtr);
			}
		}
		{
			const int rightChild = (startNode << 1) + 1;
			if (rightChild < Tree.size() && Tree[rightChild].Start <= point && Tree[rightChild].End > point) {
				FindContainingIntervals(point, rightChild, callbackPtr);
			}
		}
	}

	void PutInterval(const TKey& start, const TKey& end, const TValue& value, const int startNode) {
		const TKey& nodeStart = Tree.at(startNode).Start;
		const TKey& nodeEnd = Tree.at(startNode).End;
		if (nodeStart == start && nodeEnd == end) {
			Tree.at(startNode).Values.push_back(value);
			return;
		}
		const int leftChild = startNode << 1;
		const int rightChild = leftChild + 1;
		const TKey& leftChildEnd = Tree.at(leftChild).End;
		if (start < leftChildEnd) {
			TSegmentTree::PutInterval(start, std::min(leftChildEnd, end), value, leftChild);
		}
		if (end > leftChildEnd) {
			TSegmentTree::PutInterval(std::max(start, leftChildEnd), end, value, rightChild);
		}
	}

	struct TCounter {
		int Count;
		TCounter(): Count(0) {
		}
		void operator()(const TValue&) {
			++Count;
		}
	};

	struct TCollectorVector {
		vector<TValue> Results;
		void operator()(const TValue& value) {
			Results.push_back(value);
		}
	};
	struct TCollectorSet {
		set<TValue> Results;
		void operator()(const TValue& value) {
			Results.insert(value);
		}
	};

};


#endif
