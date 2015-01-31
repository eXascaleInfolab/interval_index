#include <vector>
#include <time.h>
#include <chrono>
#include <iostream>
#include <fstream>
#include <string>
#include <algorithm>
#include <unistd.h>
#include <sstream>
#include <set>
#include <algorithm>
#include <stdio.h>


using std::vector;
using std::string;
using std::cout;
using std::ifstream;
using std::pair;
using std::set;






template <class TIntervalBorder, class TValue>
class TIntervalIndex {
public:
	typedef std::pair<TIntervalBorder, TIntervalBorder> TInterval;
	typedef std::pair<TInterval, TValue> TKeyId;
	~TIntervalIndex() {
	}
	TIntervalIndex(const vector<TKeyId>& data, const double spaceFactor=1.0, const int checkpointInterval = -1)
													: CheckpointInterval(checkpointInterval)
													, Index(data) {
		std::sort(Index.begin(), Index.end());
		BoundingInterval.first = Index.size() ? Index[0].first.first : 0;
		BoundingInterval.second = BoundingInterval.first;
		for (int intervalIndex = 0; intervalIndex < Index.size(); ++intervalIndex) {
			if (BoundingInterval.second < Index[intervalIndex].first.second) {
				BoundingInterval.second = Index[intervalIndex].first.second;
			}
		}
		if (CheckpointInterval == -1) {//optimal chi
			vector<TIntervalBorder> inside;
			vector<int> overlappings;
			double avgOverlapping = 0;
			for (int intervalIndex = 0; intervalIndex < Index.size(); ++intervalIndex) {
				{
					TIntervalBorder removeAllBiggerThan = -Index[intervalIndex].first.first;
					while (inside.size()) {
						if (inside.front() <= removeAllBiggerThan) {
							break;
						}
						std::pop_heap(inside.begin(), inside.end());
						inside.pop_back();
					}
				}
				overlappings.push_back(inside.size());
				avgOverlapping += inside.size();
				inside.push_back(-Index.at(intervalIndex).first.second);
				std::push_heap(inside.begin(), inside.end());
			}
			avgOverlapping = avgOverlapping / overlappings.size();

			for (CheckpointInterval = 1; CheckpointInterval <= Index.size(); ++CheckpointInterval) {
				int numberOfCheckPoints = 0;
				long long numberOfIntervalsInCheckpointArrays = 0;
				int offset = 0;
				while (offset < Index.size()) {
					++numberOfCheckPoints;
					numberOfIntervalsInCheckpointArrays += overlappings[offset];
					offset += CheckpointInterval;
				}

				long long originalDataMem, dataStructureDataMem;
				GetMemoryConsumption(Index.size(),
									 numberOfCheckPoints,
									 numberOfIntervalsInCheckpointArrays,
								     &originalDataMem,
									 &dataStructureDataMem);

				if (dataStructureDataMem < originalDataMem * (1.0 + spaceFactor)) {
					break;
				}
			}
			//std::cout << "optimal chi:" << CheckpointInterval << "\n";

		}
		{//fill checkpoint arrays
			vector<pair<TIntervalBorder, int> > inside;
			for (int intervalIndex = 0; intervalIndex < Index.size(); ++intervalIndex) {
				{
					TIntervalBorder removeAllBiggerThan = -Index[intervalIndex].first.first;
					while (inside.size()) {
						if (inside.front().first <= removeAllBiggerThan) {
							break;
						}
						std::pop_heap(inside.begin(), inside.end());
						inside.pop_back();
					}
				}
				if (intervalIndex % CheckpointInterval == 0) {
					vector<int> orderByRightBorder;
					{
						vector<pair<TIntervalBorder, int> > insideCopy(inside.begin(), inside.end());
						while (insideCopy.size()) {
							orderByRightBorder.push_back(insideCopy.front().second);
							std::pop_heap(insideCopy.begin(), insideCopy.end());
							insideCopy.pop_back();
						}
					}
					{
						int dataStart = CheckpointsData.size();
						int toAdd = orderByRightBorder.size();
						Checkpoints.push_back(std::pair<int, int>(dataStart, toAdd));
					}
					{//push into a checkpoint array in a reversed order
						for (int index = orderByRightBorder.size() - 1; index > -1; --index) {
							CheckpointsData.push_back(Index.at(orderByRightBorder[index]));
						}
					}
				}
				{//add to inside if it overlaps next checkpoint interval
					int nextCheckpoint = CheckpointInterval * (intervalIndex / CheckpointInterval) + 1;
					if (nextCheckpoint < Index.size() &&
								Index[intervalIndex].first.second >= Index[nextCheckpoint].first.first)	{
						inside.push_back(pair<TIntervalBorder, int>(-Index[intervalIndex].first.second, intervalIndex));
						std::push_heap(inside.begin(), inside.end());
					}
				}
			}
		}
	}

	//this method is only for performance measurements
	template <class TCallback>
	int SearchBinarySearchOnly(const TIntervalBorder start, const TIntervalBorder stop, TCallback* callbackPtr=NULL) const {
		if (stop < start) {
			return 0;
		}
		if (!Index.size()) {
			return 0;
		}
		if (BoundingInterval.first > stop || BoundingInterval.second < start) {
			return 0;
		}
		int leftmostStartInsideQuery;
		if (Index.rbegin()->first.first < start) {
			leftmostStartInsideQuery = Index.size();
		} else if (Index.begin()->first.first >= start) {
			leftmostStartInsideQuery = 0;
		} else {//binary search
			int left = 0;
			int right = Index.size() - 1;
			while (right > left + 1) {
				int middle = (left + right) / 2;
				if (Index[middle].first.first < start) {
					left = middle;
				} else {
					right = middle;
				}
			}
			leftmostStartInsideQuery = right;
		}
		return leftmostStartInsideQuery;
	}


	//this method is only for performance measurements
	template <class TCallback>
	void SearchStartingBeforeOnly(const TIntervalBorder start, const TIntervalBorder stop, TCallback* callbackPtr=NULL,
							bool onlyBinarySearch=false, bool skipIntervalsStartedBeforeQuery=false) const {
		if (stop < start) {
			return;
		}
		if (!Index.size()) {
			return;
		}
		if (BoundingInterval.first > stop || BoundingInterval.second < start) {
			return;
		}
		int leftmostStartInsideQuery;
		if (Index.rbegin()->first.first < start) {
			leftmostStartInsideQuery = Index.size();
		} else if (Index.begin()->first.first >= start) {
			leftmostStartInsideQuery = 0;
		} else {//binary search
			int left = 0;
			int right = Index.size() - 1;
			while (right > left + 1) {
				int middle = (left + right) / 2;
				if (Index[middle].first.first < start) {
					left = middle;
				} else {
					right = middle;
				}
			}
			leftmostStartInsideQuery = right;
		}
		int checkpointIndex = 0;
		if (leftmostStartInsideQuery == 0 || leftmostStartInsideQuery % CheckpointInterval > 0
										  || start == Index[leftmostStartInsideQuery].first.first) {
			checkpointIndex = leftmostStartInsideQuery / CheckpointInterval;
		} else {
			// because there might be an interval which overlaps query,
			//   but ends BEFORE leftmostStartInsideQuery's start
			checkpointIndex = (leftmostStartInsideQuery - 1) / CheckpointInterval;
		}
		checkpointIndex = std::min(checkpointIndex, (int)Checkpoints.size() - 1);
		const int checkpointPosition = CheckpointInterval * checkpointIndex;
		if (leftmostStartInsideQuery > 0) {//checkpoint_position -> start_point
			for (int current = checkpointPosition; current < leftmostStartInsideQuery; ++current) {
				if (Index[current].first.second >= start) {
					if (callbackPtr) {
						(*callbackPtr)(Index[current].first, Index[current].second);
					}
				}
			}
		}
		{//checkpoint intervals
			int dataStart = Checkpoints[checkpointIndex].first;
			int dataEnd = dataStart + Checkpoints[checkpointIndex].second;
			for (int position = dataStart; position < dataEnd; ++position) {
				if (CheckpointsData[position].first.second >= start) {
					if (callbackPtr) {
						(*callbackPtr)(CheckpointsData[position].first, CheckpointsData[position].second);
					}
				} else {
					break; // they are sorted by right border
				}
			}
		}
	}
	//this method is only for performance measurements
	template <class TCallback>
	void SearchExceptCheckpointArray(const TIntervalBorder start, const TIntervalBorder stop, TCallback* callbackPtr=NULL,
							bool onlyBinarySearch=false, bool skipIntervalsStartedBeforeQuery=false) const {
		if (stop < start) {
			return;
		}
		if (!Index.size()) {
			return;
		}
		if (BoundingInterval.first > stop || BoundingInterval.second < start) {
			return;
		}
		int leftmostStartInsideQuery;
		if (Index.rbegin()->first.first < start) {
			leftmostStartInsideQuery = Index.size();
		} else if (Index.begin()->first.first >= start) {
			leftmostStartInsideQuery = 0;
		} else {//binary search
			int left = 0;
			int right = Index.size() - 1;
			while (right > left + 1) {
				int middle = (left + right) / 2;
				if (Index[middle].first.first < start) {
					left = middle;
				} else {
					right = middle;
				}
			}
			leftmostStartInsideQuery = right;
		}
		int checkpointIndex = 0;
		if (leftmostStartInsideQuery == 0 || leftmostStartInsideQuery % CheckpointInterval > 0
										  || start == Index[leftmostStartInsideQuery].first.first) {
			checkpointIndex = leftmostStartInsideQuery / CheckpointInterval;
		} else {
			// because there might be an interval which overlaps query,
			//   but ends BEFORE leftmostStartInsideQuery's start
			checkpointIndex = (leftmostStartInsideQuery - 1) / CheckpointInterval;
		}
		checkpointIndex = std::min(checkpointIndex, (int)Checkpoints.size() - 1);
		const int checkpointPosition = CheckpointInterval * checkpointIndex;
		//checkpoint_position -> start_point
		if (leftmostStartInsideQuery > 0) {
			for (int current = checkpointPosition; current < leftmostStartInsideQuery; ++current) {
				if (Index[current].first.second >= start) {
					if (callbackPtr) {
						(*callbackPtr)(Index[current].first, Index[current].second);
					}
				}
			}
		}
		{//start_point -> (till left border goes beyond stop value)
			for (int current = leftmostStartInsideQuery;
					 current < Index.size() && Index[current].first.first <= stop; ++current) {
				if (callbackPtr) {
					(*callbackPtr)(Index[current].first, Index[current].second);
				}
			}
		}
	}



	//this method is only for performance measurements
	template <class TCallback>
	void BinSearchWalkToCheckpointOnly(const TIntervalBorder start, const TIntervalBorder stop, TCallback* callbackPtr=NULL,
							bool onlyBinarySearch=false, bool skipIntervalsStartedBeforeQuery=false) const {
		if (stop < start) {
			return;
		}
		if (!Index.size()) {
			return;
		}
		if (BoundingInterval.first > stop || BoundingInterval.second < start) {
			return;
		}
		int leftmostStartInsideQuery;
		if (Index.rbegin()->first.first < start) {
			leftmostStartInsideQuery = Index.size();
		} else if (Index.begin()->first.first >= start) {
			leftmostStartInsideQuery = 0;
		} else {//binary search
			int left = 0;
			int right = Index.size() - 1;
			while (right > left + 1) {
				int middle = (left + right) / 2;
				if (Index[middle].first.first < start) {
					left = middle;
				} else {
					right = middle;
				}
			}
			leftmostStartInsideQuery = right;
		}
		int checkpointIndex = 0;
		if (leftmostStartInsideQuery == 0 || leftmostStartInsideQuery % CheckpointInterval > 0
										  || start == Index[leftmostStartInsideQuery].first.first) {
			checkpointIndex = leftmostStartInsideQuery / CheckpointInterval;
		} else {
			// because there might be an interval which overlaps query,
			//   but ends BEFORE leftmostStartInsideQuery's start
			checkpointIndex = (leftmostStartInsideQuery - 1) / CheckpointInterval;
		}
		checkpointIndex = std::min(checkpointIndex, (int)Checkpoints.size() - 1);
		const int checkpointPosition = CheckpointInterval * checkpointIndex;
		if (leftmostStartInsideQuery > 0) {//checkpoint_position -> start_point
			for (int current = checkpointPosition; current < leftmostStartInsideQuery; ++current) {
				if (Index[current].first.second >= start) {
					if (callbackPtr) {
						(*callbackPtr)(Index[current].first, Index[current].second);
					}
				}
			}
		}
	}


	template <class TCallback>
	void Search(const TIntervalBorder start, const TIntervalBorder stop, TCallback* callbackPtr=NULL) const {
		if (stop < start) {
			return;
		}
		if (!Index.size()) {
			return;
		}
		if (BoundingInterval.first > stop || BoundingInterval.second < start) {
			return;
		}
		int leftmostStartInsideQuery;
		if (Index.rbegin()->first.first < start) {
			leftmostStartInsideQuery = Index.size();
		} else if (Index.begin()->first.first >= start) {
			leftmostStartInsideQuery = 0;
		} else {//binary search
			int left = 0;
			int right = Index.size() - 1;
			while (right > left + 1) {
				int middle = (left + right) / 2;
				if (Index[middle].first.first < start) {
					left = middle;
				} else {
					right = middle;
				}
			}
			leftmostStartInsideQuery = right;
		}
		int checkpointIndex = 0;
		if (leftmostStartInsideQuery == 0 || leftmostStartInsideQuery % CheckpointInterval > 0
										  || start == Index[leftmostStartInsideQuery].first.first) {
			checkpointIndex = leftmostStartInsideQuery / CheckpointInterval;
		} else {
			// because there might be an interval which overlaps query,
			//   but ends BEFORE leftmostStartInsideQuery's start
			checkpointIndex = (leftmostStartInsideQuery - 1) / CheckpointInterval;
		}
		checkpointIndex = std::min(checkpointIndex, (int)Checkpoints.size() - 1);
		const int checkpointPosition = CheckpointInterval * checkpointIndex;
		{//checkpoint_position -> start_point
			for (int current = checkpointPosition; current < leftmostStartInsideQuery; ++current) {
				if (Index[current].first.second >= start) {
					if (callbackPtr) {
						(*callbackPtr)(Index[current].first, Index[current].second);
					}
				}
			}
		}
		{//start_point -> (till left border goes beyond stop value)
			for (int current = leftmostStartInsideQuery;
					 current < Index.size() && Index[current].first.first <= stop; ++current) {
				if (callbackPtr) {
					(*callbackPtr)(Index[current].first, Index[current].second);
				}
			}
		}
		{//checkpoint intervals
			int dataStart = Checkpoints[checkpointIndex].first;
			int dataEnd = dataStart + Checkpoints[checkpointIndex].second;
			for (int position = dataStart; position < dataEnd; ++position) {
				if (CheckpointsData[position].first.second >= start) {
					if (callbackPtr) {
						(*callbackPtr)(CheckpointsData[position].first, CheckpointsData[position].second);
					}
				} else {
					break; // they are sorted by right border
				}
			}
		}
	}

	void GetMemoryConsumption(long long* originalDataPtr, long long* dataStructurePtr) const {
		*originalDataPtr = Index.size() * sizeof(TKeyId);
		*dataStructurePtr = *originalDataPtr;
		*dataStructurePtr += CheckpointsData.size() * sizeof(TKeyId);
		*dataStructurePtr += Checkpoints.size() * sizeof(std::pair<int, int>);
	}

	void GetMemoryConsumption(const long long indexSize,
							  const long long numberOfCheckpoints,
							  const long long totalSizeOfCheckpointArrays,
							  long long* originalDataPtr,
							  long long* dataStructurePtr) const {
		*originalDataPtr = indexSize * sizeof(TKeyId);
		*dataStructurePtr = *originalDataPtr;
		*dataStructurePtr += totalSizeOfCheckpointArrays * sizeof(TKeyId);
		*dataStructurePtr += numberOfCheckpoints * sizeof(std::pair<int, int>);
	}

	int GetCheckpointInterval() const {
		return CheckpointInterval;
	}

private:
	int CheckpointInterval;
	vector<TKeyId> Index;
	vector<TKeyId> CheckpointsData;
	vector<std::pair<int, int> > Checkpoints;
	TInterval BoundingInterval;


};





