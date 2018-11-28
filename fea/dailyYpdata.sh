spark-submit --master yarn-client --num-executors 15 --executor-memory 12G --executor-cores 10 --conf spark.default.parallelism=300 min.py 
spark-submit --master yarn-client --num-executors 15 --executor-memory 12G --executor-cores 10 --conf spark.default.parallelism=300 max.py 
spark-submit --master yarn-client --num-executors 15 --executor-memory 12G --executor-cores 10 --conf spark.default.parallelism=300 avg.py 
spark-submit --master yarn-client --num-executors 15 --executor-memory 12G --executor-cores 10 --conf spark.default.parallelism=300 sum_avg.py 


spark-submit --master yarn-client --num-executors 15 --executor-memory 12G --executor-cores 10 --conf spark.default.parallelism=300 count_all.py 
spark-submit --master yarn-client --num-executors 15 --executor-memory 25G --executor-cores 4 --conf spark.default.parallelism=200 count_row_num.py 
spark-submit --master yarn-client --num-executors 15 --executor-memory 25G --executor-cores 4 --conf spark.default.parallelism=200 count_day_pay_1.py 
spark-submit --master yarn-client --num-executors 15 --executor-memory 25G --executor-cores 4 --conf spark.default.parallelism=200 count_day_pay_2.py 
spark-submit --master yarn-client --num-executors 15 --executor-memory 25G --executor-cores 4 --conf spark.default.parallelism=200 count_day_open_avg_1.py 
spark-submit --master yarn-client --num-executors 15 --executor-memory 25G --executor-cores 4 --conf spark.default.parallelism=200 count_day_open_avg_2.py 

spark-submit --master yarn-client --num-executors 15 --executor-memory 25G --executor-cores 4 --conf spark.default.parallelism=200 third_count_1.py 
spark-submit --master yarn-client --num-executors 15 --executor-memory 25G --executor-cores 4 --conf spark.default.parallelism=200 third_count_2.py 
spark-submit --master yarn-client --num-executors 15 --executor-memory 25G --executor-cores 4 --conf spark.default.parallelism=200 third_count_3.py 
spark-submit --master yarn-client --num-executors 15 --executor-memory 12G --executor-cores 10 --conf spark.default.parallelism=300 third_2.py 
















