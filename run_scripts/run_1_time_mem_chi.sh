rm index_run
g++-4.8 -O3 1_time_mem_chi.cpp  -std=gnu++11 -o index_run
filename=../datasets/chi_time_mem_1M_100_1M_100.txt
last_algo=`./index_run -wcount`
last_algo=`expr $last_algo - 1` 
for run in `seq 1 30`
do  
  echo launch $run
  for algo_index in `seq 0 $last_algo`
  do 
    ./index_run $filename $algo_index
  done
done

