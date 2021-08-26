module purge
module load python/gcc/3.7.9

hdfs dfs -put data/btc/* data/btc
hdfs dfs -put data/eth/* data/eth
hdfs dfs -put data/ltc/* data/ltc