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




void UploadData(const char* filename, vector<TKeyId>* intervalsPtr, vector<TInterval>* queriesPtr) {
	std::ifstream stream (filename, std::ifstream::binary);
	if (!stream) {
		return;
	}
	int intervalsCount;
	stream >> intervalsCount;
	for (int interval_index = 0; interval_index < intervalsCount; ++interval_index) {
		TIntervalBorder start, end;
		TValue id;
		stream >> start >> end >> id;
		intervalsPtr->push_back(TKeyId(TInterval(start, end), id));
	}
	int queriesCount;
	stream >> queriesCount;
	for (int queryIndex = 0; queryIndex < queriesCount; ++queryIndex) {
		TIntervalBorder start, end;
		stream >> start >> end;
		queriesPtr->push_back(TInterval(start, end));
	}
}



int main(int argc, char *argv[]) {

	string datasetPath = argv[1];
	vector<TKeyId> data;
	vector<TInterval> queries;
	UploadData(datasetPath.c_str(), &data, &queries);


	vector<TKeyId> buffer;
	for (int i = 0; i < 10; ++i) {
		buffer.insert(buffer.end(), data.begin(), data.end());
	}


	/*
	{
		vector<std::shared_ptr<TIndividualIntervalIndexTester> > wrappers;
		wrappers.push_back(std::shared_ptr<TIndividualIntervalIndexTester>(new TIndividualIntervalIndexTester("MavlyutovIndex_x1", 1, 1)));
		wrappers.push_back(std::shared_ptr<TIndividualIntervalIndexTester>(new TIndividualIntervalIndexTester("MavlyutovIndex_x10", 1, 10)));
		wrappers.push_back(std::shared_ptr<TIndividualIntervalIndexTester>(new TIndividualIntervalIndexTester("MavlyutovIndex_x100", 1, 100)));
		wrappers.push_back(std::shared_ptr<TIndividualIntervalIndexTester>(new TIndividualIntervalIndexTester("MavlyutovIndex_x1000", 1, 1000)));

		long long totalHitsCount = 0;
		for (auto wrapperIt = wrappers.begin(); wrapperIt != wrappers.end(); ++wrapperIt) {
			double memUsageKb;
			double buildTimeMilliSec;
			wrapperIt->get()->Build(data, &buildTimeMilliSec, &memUsageKb);
			//wrapperIt->get()->TestQuality(data, queries);
			long long hitsCount = 0;

			double binSearchTime, noInsideTime, queryTime;
			wrapperIt->get()->CalcQueryTime(queries, &hitsCount, &binSearchTime, &noInsideTime, &queryTime);
			std::cout << datasetPath << "\t" << wrapperIt->get()->Id << "\t" <<  wrapperIt->get()->IntervalIndexPtr->GetVariableMemoryConsumption()
						  <<  "\t" << hitsCount
						  << "\t" << binSearchTime
						  << "\t" << noInsideTime
						  << "\t" << queryTime << "\n";
			std::cout.flush();
			wrapperIt->get()->Clear();
			if (wrapperIt == wrappers.begin()) {
				totalHitsCount = hitsCount;
			} else if (totalHitsCount != hitsCount) {
				std::cerr << "Mismatched number of results: " << totalHitsCount << " >< " <<  hitsCount << "\n";
				//exit(1);
			}
		}
	}
	*/

	{
		vector<std::shared_ptr<TIntervalIndexTester> > wrappers;

		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("free", 1, 1)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x001", 1, 1)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x002", 1, 1)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x003", 1, 1)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x004", 1, 1)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x005", 1, 1)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x010", 1, 10)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x020", 1, 20)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x030", 1, 30)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x050", 1, 50)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x080", 1, 80)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x120", 1, 120)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x150", 1, 150)));
		wrappers.push_back(std::shared_ptr<TIntervalIndexTester>(new TIntervalIndexTester("x180", 1, 180)));

		/*
		for (int chi = 1; chi < 10; chi += 1) {
			string name = "x";
			if (chi < 10) name += "0";
			if (chi < 100) name += "0";
			name += std::to_string(chi);
			wrappers.push_back(std::shared_ptr<TIndividualIntervalIndexTester>(new TIndividualIntervalIndexTester(name, 1, chi)));
		}
		for (int chi = 10; chi < 100; chi += 6) {
			string name = "x";
			if (chi < 10) name += "0";
			if (chi < 100) name += "0";
			name += std::to_string(chi);
			wrappers.push_back(std::shared_ptr<TIndividualIntervalIndexTester>(new TIndividualIntervalIndexTester(name, 1, chi)));
		}
		*/
		long long totalHitsCount = 0;
		//for (auto wrapperIt = wrappers.begin(); wrapperIt != wrappers.end(); ++wrapperIt) {
		{
			std::shared_ptr<TIntervalIndexTester>* wrapperIt = &wrappers[atoi(argv[2])];

			double memUsageKb;
			double buildTimeMilliSec;
			wrapperIt->get()->Build(data, &buildTimeMilliSec, &memUsageKb);
			//wrapperIt->get()->TestQuality(data, queries);
			long long hitsCount = 0;

			double binSearchTime, noInsideTime, queryTime;
			wrapperIt->get()->CalcQueryTime(queries, &hitsCount, &binSearchTime, &noInsideTime, &queryTime);
			long long dataMemConsumption, dsMemConsumption;
			wrapperIt->get()->IntervalIndexPtr->GetMemoryConsumption(&dataMemConsumption, &dsMemConsumption);

			std::cout << datasetPath
					      << "\t" << wrapperIt->get()->Id
						  << "\t" <<  dataMemConsumption
						  << "\t" << dsMemConsumption
						  << "\t" << hitsCount
						  << "\t" << binSearchTime
						  << "\t" << noInsideTime
						  << "\t" << queryTime << "\n";
			std::cout.flush();

			/*
			wrapperIt->get()->Clear();
			if (wrapperIt == wrappers.begin()) {
				totalHitsCount = hitsCount;
			} else if (totalHitsCount != hitsCount) {
				std::cerr << "Mismatched number of results: " << totalHitsCount << " >< " <<  hitsCount << "\n";
				//exit(1);
			}
			*/
		}

	}





	return 0;
}
