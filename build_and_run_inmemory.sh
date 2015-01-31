set -e

mkdir -p bin
mkdir -p datasets
mkdir -p test_results

rm -f index_run
g++-4.8 -O3 src/main.cpp src/nclist/intervaldb.c -Isrc/ -std=gnu++11 -o bin/index_run
echo "BINARY BUILT SUCCESSFUL"

echo "BUILDING"
python python/build_synth_datasets.py 

echo "RUNNING"
sh run_scripts/run.sh datasets/query_len/ > test_results/query_len.txt 

echo "Saved to test_results/*txt"
