#pragma once
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
#include <time.h>
#include <set>
#include <unordered_set>
#include <unistd.h>
#include <ios>
using std::vector;
using std::string;
using std::cout;
using std::ifstream;
using std::pair;
using std::set;


//shortcuts
#define TSharedPtr std::shared_ptr
typedef double TIntervalBorder;
typedef int TValue;
typedef std::pair<TIntervalBorder, TIntervalBorder> TInterval;
typedef std::pair<TInterval, TValue> TKeyId;


//////////////////////////////////////////////////////////////////////////////
//
// process_mem_usage(double &, double &) - takes two doubles by reference,
// attempts to read the system-dependent data for a process' virtual memory
// size and resident set size, and return the results in KB.
//
// On failure, returns 0.0, 0.0

void process_mem_usage(double& vm_usage, double& resident_set)
{
   using std::ios_base;
   using std::ifstream;
   using std::string;

   vm_usage     = 0.0;
   resident_set = 0.0;

   // 'file' stat seems to give the most reliable results
   //
   ifstream stat_stream("/proc/self/stat",ios_base::in);

   // dummy vars for leading entries in stat that we don't care about
   //
   string pid, comm, state, ppid, pgrp, session, tty_nr;
   string tpgid, flags, minflt, cminflt, majflt, cmajflt;
   string utime, stime, cutime, cstime, priority, nice;
   string O, itrealvalue, starttime;

   // the two fields we want
   //
   unsigned long vsize;
   long rss;

   stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr
               >> tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt
               >> utime >> stime >> cutime >> cstime >> priority >> nice
               >> O >> itrealvalue >> starttime >> vsize >> rss; // don't care about the rest

   stat_stream.close();

   long page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024; // in case x86-64 is configured to use 2MB pages
   vm_usage     = vsize / 1024.0;
   resident_set = rss * page_size_kb;
}


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


typedef std::chrono::time_point<std::chrono::system_clock> TTime;
TTime GetTime() {
    return std::chrono::system_clock::now();
}

double GetElapsedInSeconds(TTime start, TTime end) {
    std::chrono::duration<double> elapsed = end - start;
    return elapsed.count();
}
