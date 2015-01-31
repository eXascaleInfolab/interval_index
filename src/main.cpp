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


int main(int argc, char *argv[]) {
	vector<std::shared_ptr<TWrapper> > wrappers;
	wrappers.push_back(std::shared_ptr<TWrapper>(new TIntervalIndexWrapper<1>("MavlyutovIndex_x1")));
	wrappers.push_back(std::shared_ptr<TWrapper>(new TNClistWrapper("NClist")));
	wrappers.push_back(std::shared_ptr<TWrapper>(new TIntervalTreeWrapper("Interval Tree")));

	wrappers.push_back(std::shared_ptr<TWrapper>(new TRTreeWrapper<8>("R-Tree8")));
	wrappers.push_back(std::shared_ptr<TWrapper>(new TRTreeWrapper<16>("R-Tree16")));
	wrappers.push_back(std::shared_ptr<TWrapper>(new TRTreeWrapper<32>("R-Tree32")));
	wrappers.push_back(std::shared_ptr<TWrapper>(new TRTreeWrapper<64>("R-Tree64")));

	wrappers.push_back(std::shared_ptr<TWrapper>(new TSegementTreeWrapper("Segment Tree")));
	//wrappers.push_back(std::shared_ptr<TWrapper>(new TRStarTreeWrapper("R*-Tree")));

	if (string(argv[1]) == "-wcount") {
		std::cout << wrappers.size() << "\n";
		return 0;
	}

	string datasetPath = argv[1];
	const int algo2use = atoi(argv[2]);
	vector<TKeyId> data;
	vector<TInterval> queries;
	UploadData(datasetPath.c_str(), &data, &queries);

	if (0)
	{//warmup
	    vector<TKeyId> buffer;
	    for (int i = 0; i < 5; ++i) {
	        buffer.insert(buffer.end(), data.begin(), data.end());
	    }
	    std::cout << "warmup: " << buffer.size() << "\n";
	}

	vector<std::shared_ptr<TWrapper> >::iterator wrapperIt = wrappers.begin() + algo2use;
	double memUsageKb;
	double buildTimeSec;
	wrapperIt->get()->Build(data, &buildTimeSec, &memUsageKb);
	long long hitsCount = 0;
	double queryTime = wrapperIt->get()->CalcQueryTime(queries, &hitsCount);
	std::cout << datasetPath << "\t" << wrapperIt->get()->Id << "\t" << hitsCount << "\t" << memUsageKb << "\t" << buildTimeSec <<  "\t" << queryTime << "\n";

	return 0;
}
