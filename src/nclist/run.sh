HDT_INCLUDE=""
DEBUG=""

g++ $DEBUG $HDT_INCLUDE nclist.cpp -o main.o -std=gnu++11 -c
g++ $DEBUG $HDT_INCLUDE intervaldb.c -std=gnu++11 -c

g++ -o tester *.o 
rm *.o
./tester
