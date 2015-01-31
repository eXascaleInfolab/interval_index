algos_count=`bin/index_run -wcount`
last_algo=`expr $algos_count - 1` 
for run_index in 0 1 2 3 4 5 6 7 8 9 
do 
  echo run$run_index
  for path in `ls $1/*.txt`
  do
    for algo_index in `seq 0 $last_algo`
    do
      bin/index_run $path $algo_index 
    done
  done
done

