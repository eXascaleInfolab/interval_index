rm index_run
g++-4.8 -O3 main_only_ii.cpp nclist/intervaldb.c ../tools/papi-5.3.2/src/libpapi.a -I../tools/papi-5.3.2/src/ -std=gnu++11 -o index_run

for i in 1 2 3 4 5 6 7 8 9 10
do 
   for path in `ls $1/*.txt`
   do
       echo $path
       ./index_run $path 
   done
done

