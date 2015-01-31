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
#include <memory>
#include "wrappers.hpp"

using std::vector;
using std::string;
using std::cout;
using std::ifstream;
using std::pair;
using std::set;



int main(int argc, char *argv[]) {
	string datasetPath = argv[1];
	vector<TKeyId> data;
	vector<TInterval> queries;
	UploadData(datasetPath.c_str(), &data, &queries);
	{
	    vector<TKeyId> buffer;
	    for (int i = 0; i < 20; ++i) {
	        buffer.insert(buffer.end(), data.begin(), data.end());
	    }
	    std::cout << buffer.size() << "\n";
	}
	{
		TIntervalIndexTester tester("free", 1);
		tester.Build(data);
		long long dataMemConsumption, dsMemConsumption;
		tester.IntervalIndexPtr->GetMemoryConsumption(&dataMemConsumption, &dsMemConsumption);
		long long hitsCount = 0, hitsStartedBeforeQueryCount = 0, hitsWalkToCheckpoint = 0;
		double binSearchTime, binSearchAndWalkToChi, noInsideTime, fullQueryTime;
		tester.CalcQueryTime(queries,
								&hitsWalkToCheckpoint,
								&hitsStartedBeforeQueryCount,
								&hitsCount,
							 &binSearchTime,
							 &binSearchAndWalkToChi,
							 &noInsideTime,
							 &fullQueryTime);
		std::cout << datasetPath
					  << "\t" << tester.Id
					  << "\t" << dataMemConsumption
					  << "\t" << dsMemConsumption
					  << "\t" << hitsWalkToCheckpoint
					  << "\t" << hitsStartedBeforeQueryCount
					  << "\t" << hitsCount
					  << "\t" << tester.IntervalIndexPtr->GetCheckpointInterval()
					  << "\t" << binSearchTime
					  << "\t" << binSearchAndWalkToChi
					  << "\t" << noInsideTime
					  << "\t" << fullQueryTime << "\n";
		std::cout.flush();
	}

	return 0;
}
