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
	vector<std::shared_ptr<TIntervalIndexTester> > wrappers;
	{
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("free", 1)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x001", 1, 1)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x005", 1, 5)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x010", 1, 10)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x020", 1, 20)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x030", 1, 30)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x040", 1, 40)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x050", 1, 50)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x080", 1, 80)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x110", 1, 110)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x140", 1, 140)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x180", 1, 180)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x200", 1, 200)));
	}
	if (string(argv[1]) == "-wcount") {
		std::cout << wrappers.size();
		return 0;
	}

	string datasetPath = argv[1];
	const int algoToRun = atoi(argv[2]);

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
		std::shared_ptr<TIntervalIndexTester>* wrapperIt = &wrappers[algoToRun];
		wrapperIt->get()->Build(data);
		long long dataMemConsumption, dsMemConsumption;
		wrapperIt->get()->IntervalIndexPtr->GetMemoryConsumption(&dataMemConsumption, &dsMemConsumption);
		long long resultsCounter = 0;
		long long resultsOutOfCheckpointArray = 0;
		long long resultsFromWalkToCheckpoint = 0;
		double binSearchTime, binSearchAndWalkToCheckpointTime, noCheckpointArray, fullQueryTime;
		wrapperIt->get()->CalcQueryTime(queries,
										&resultsFromWalkToCheckpoint,
										&resultsOutOfCheckpointArray,
										&resultsCounter,
										&binSearchTime,
										&binSearchAndWalkToCheckpointTime,
										&noCheckpointArray,
										&fullQueryTime);
		std::cout << datasetPath
					  << "\t" << wrapperIt->get()->Id
					  << "\t" << dataMemConsumption
					  << "\t" << dsMemConsumption
					  << "\t" << resultsFromWalkToCheckpoint
					  << "\t" << resultsOutOfCheckpointArray
					  << "\t" << resultsCounter
					  << "\t" << wrapperIt->get()->IntervalIndexPtr->GetCheckpointInterval()
					  << "\t" << binSearchTime
					  << "\t" << binSearchAndWalkToCheckpointTime
					  << "\t" << noCheckpointArray
					  << "\t" << fullQueryTime << "\n";
		std::cout.flush();
	}

	return 0;
}
