//============================================================================
// Name        : nclist.cpp
// Author      : Ruslan Mavlyutov 
// Version     :
// Copyright   : Your copyright notice
// Description : Hello World in C++, Ansi-style
//============================================================================

#include "intervaldb.h"
#include <iostream>
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
#include <random>

using std::vector;
using std::string;
using std::cout;
using std::ifstream;
using std::pair;
using std::set;

typedef std::chrono::time_point<std::chrono::system_clock> TTime;
TTime GetTime() {
    return std::chrono::system_clock::now();
}

double GetElapsedInSeconds(TTime start, TTime end) {
    std::chrono::duration<double> elapsed = end - start;
    return elapsed.count();
}

typedef std::pair<int, int> TInterval;
typedef std::pair<TInterval, int> TKeyId;


void Callback(int id) {
}

const int CHECKPOINTS_INTERVAL_DEFAULT = 1000;

class TIntervalIndex {
public:
	TIntervalIndex(vector<TKeyId>& data) : Index(data), CheckpointsInterval(CHECKPOINTS_INTERVAL_DEFAULT) {
		std::sort(Index.begin(), Index.end());

		if (Index.size()) {// correct checkpoints interval with average overlapping
			double total = 0.0;
			int componentsCount = 0;
			vector<TKeyId> inside;
			for (int pointIndex = 0; pointIndex < Index.size(); ++pointIndex) {
				inside.push_back(Index[pointIndex]);
				if (pointIndex % CheckpointsInterval == 0) {
					{//leave only intervals that continue
						vector<TKeyId> filtered;
						for (int index = 0; index < inside.size(); ++index) {
							if (inside[index].first.second > Index[pointIndex].first.first) {
								filtered.push_back(inside[index]);
							}
						}
						std::swap(filtered, inside);
						total += inside.size();
						++componentsCount;
					}
				}
			}
			int average = total / componentsCount;
			CheckpointsInterval = average + 1;
		}

		vector<TKeyId> inside;
		for (int pointIndex = 0; pointIndex < Index.size(); ++pointIndex) {
			inside.push_back(Index[pointIndex]);
			if (pointIndex % CheckpointsInterval == 0) {
				{//leave only intervals that continue
					vector<TKeyId> filtered;
					for (int index = 0; index < inside.size(); ++index) {
						if (inside[index].first.second > Index[pointIndex].first.first) {
							filtered.push_back(inside[index]);
						}
					}
					std::swap(filtered, inside);
				}
				{//sort by end point
					vector<std::pair<int, TKeyId> > sortVector;
					for (int index = 0; index < inside.size(); ++index) {
						sortVector.push_back(std::pair<int, TKeyId> (inside[index].first.second, inside[index])  );
					}
					std::sort(sortVector.begin(), sortVector.end());
					for (int index = 0; index < inside.size(); ++index) {
						inside[index] = sortVector[index].second;
					}
				}
				Checkpoints.push_back(inside);
			}
		}
	}

	int Search(const int start, const int end) {
		if (end <= start) {
			return 0;
		}
		int startPoint;
		{
			int left = 0;
			int right = Index.size() - 1;
			if (Index[left].first.first >= end || Index[right].first.second <= start) {
				return 0;
			}
			while (right > left + 1) {
				int middle = (left + right) / 2;
				if (Index[middle].first.first <= start) {
					left = middle;
				} else {
					right = middle;
				}
			}
			startPoint = left;
	        while (startPoint > 0 && Index[startPoint - 1].first.first == Index[left].first.first) {
	        	--startPoint;
	        }
		}
		int checkpointIndex = startPoint / CheckpointsInterval;
		int inside = 0;
		for (int index = Checkpoints[checkpointIndex].size() - 1; index >= 0; --index) {
			if (Checkpoints[checkpointIndex][index].first.second > start) {
				Callback(Checkpoints[checkpointIndex][index].second);
				++inside;
			} else {
				break;
			}
		}
		int current = checkpointIndex * CheckpointsInterval + 1;
        while (current < Index.size() && Index[current].first.first < end) {
        	if (Index[current].first.second > start) {
        		int id = Index[current].second;
        		Callback(id);
        		++inside;
        	}
            current += 1;
        }
        return inside;
	}

private:
	struct TIndexPoint {
		TIndexPoint(int borderValue, int idValue) : BorderValue(borderValue),
													IdValue(idValue) {
		}
		int BorderValue;
		int IdValue;
	};
	vector<TKeyId> Index;
	vector<vector<TKeyId> > Checkpoints;
public:
	int CheckpointsInterval;
};




int main() {

	const int INTERVALS_COUNT = 10000;
	IntervalMap buffer[INTERVALS_COUNT];


	vector<IntervalMap> source(INTERVALS_COUNT);
	for (int intervalIndex = 0; intervalIndex < INTERVALS_COUNT; ++intervalIndex) {
		source[intervalIndex].start = intervalIndex;
		source[intervalIndex].end = intervalIndex + 100;
		source[intervalIndex].target_id = intervalIndex;
		source[intervalIndex].target_start = intervalIndex;
		source[intervalIndex].target_end = intervalIndex;
		source[intervalIndex].sublist = -1;
	}
	//IntervalDB* dbPtr = build_interval_db(source, INTERVALS_COUNT);
	int p_n, p_nlists;
	SublistHeader* headerPtr = build_nested_list(&source[0], INTERVALS_COUNT, &p_n, &p_nlists);
	std::cout << p_n << " " << p_nlists << "\n";




	int MAX_QUERIES = 10000;
	int MAX_VALUE = 10000000;

	for (int QUERY_WINDOWS = 100; QUERY_WINDOWS < 100000; QUERY_WINDOWS = QUERY_WINDOWS << 1) {
	for (int INTERVAL_MAX_LENGTH = 100; INTERVAL_MAX_LENGTH < 100000; INTERVAL_MAX_LENGTH = INTERVAL_MAX_LENGTH << 1) {
	for (int MAX_POINTS = 1000; MAX_POINTS < 5000000; MAX_POINTS = MAX_POINTS << 1) {

			vector<TKeyId> points;
			vector<IntervalMap> points4Nclist;
			vector<std::pair<int, int> > queries;
			{
				srand (time(NULL));
				for (int point_index = 0; point_index < MAX_POINTS; ++point_index) {
					int start = MAX_VALUE * (float)rand() / RAND_MAX;
					int length = INTERVAL_MAX_LENGTH * (float)rand() / RAND_MAX + 1;
					TInterval interval(start, start + length);
					points.push_back(TKeyId(interval, point_index + 1));
					{
						IntervalMap interval;
						interval.start = start;
						interval.end = start + length;
						interval.sublist = -1;
						interval.target_id = point_index + 1;
						interval.target_start = 0;
						interval.target_end = 0;
						points4Nclist.push_back(interval);
					}

				}
				for (int query_index = 0; query_index < MAX_QUERIES; ++query_index) {
					int start = MAX_VALUE * (float)rand() / RAND_MAX;
					int end = start + QUERY_WINDOWS;
					queries.push_back(std::pair<int, int>(start, end));
				}
			}

			int totalHits = 0;
			if (0) {
				for (int query_index = 0; query_index < queries.size(); ++query_index) {
					int start = queries[query_index].first;
					int end = queries[query_index].second;
					for (int intervalIndex = 0; intervalIndex < points.size(); ++intervalIndex) {
						if (points[intervalIndex].first.first < end && points[intervalIndex].first.second > start) {
							++totalHits;
						}
					}
				}
			}



			int checkpointsInterval;

			double intervalIndexCreate, intervalIndexQuery;
			int intervalIndexHitsCount = 0;
			{
				TTime startTime = GetTime();
				TIntervalIndex index(points);
				checkpointsInterval = index.CheckpointsInterval;

				intervalIndexCreate = GetElapsedInSeconds(startTime, GetTime());
				startTime = GetTime();
				for (int query_index = 0; query_index < queries.size(); ++query_index) {

					int start = queries[query_index].first;
					int end = queries[query_index].second;
					intervalIndexHitsCount += index.Search(start, end);
				}
				intervalIndexQuery = GetElapsedInSeconds(startTime, GetTime());
			}

			double rTreeCreate, rTreeQuery;
			int nclistHitsCount = 0;
			{
				TTime startTime = GetTime();

				int p_n, p_nlists;
				SublistHeader* headerPtr = build_nested_list(&points4Nclist[0], points4Nclist.size(), &p_n, &p_nlists);
				cout << p_nlists << "\n";

				rTreeCreate = GetElapsedInSeconds(startTime, GetTime());
				startTime = GetTime();

				for (int query_index = 0; query_index < queries.size(); ++query_index) {
					int start = queries[query_index].first;
					int end = queries[query_index].second;
					IntervalIterator* iterator = 0;

					int p_nreturn;
					find_intervals(iterator, start, end, &points4Nclist[0], p_n, headerPtr, p_nlists, buffer, INTERVALS_COUNT, &p_nreturn, &iterator);
					nclistHitsCount += p_nreturn;
				}
				rTreeQuery = GetElapsedInSeconds(startTime, GetTime());

				delete[] headerPtr;

			}

			cout << checkpointsInterval << "\t" << MAX_POINTS << "\t" << INTERVAL_MAX_LENGTH << "\t" << QUERY_WINDOWS << "\t" << totalHits << "\t";
			cout << intervalIndexCreate << "\t" << intervalIndexQuery << "\t" << intervalIndexHitsCount << "\t";
			cout << rTreeCreate << "\t" << rTreeQuery << "\t" << nclistHitsCount << "\n";
	}
	}
	}



    return 0;
}
