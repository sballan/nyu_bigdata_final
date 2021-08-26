module purge
module load python/gcc/3.7.9

hdfs dfs -put -f data/btc/* data/btc
hdfs dfs -put -f data/eth/* data/eth
hdfs dfs -put -f data/ltc/* data/ltc