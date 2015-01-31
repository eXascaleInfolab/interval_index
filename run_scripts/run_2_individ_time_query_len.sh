rm index_run
g++-4.8 -O3 2_individ_time_query_len.cpp -std=gnu++11 -o index_run
folder=../datasets/query_len/
for run in `seq 1 30`
do 
  echo launch $run
  for filename in `ls $folder/*.txt`
  do  
    ./index_run $filename
  done
done
