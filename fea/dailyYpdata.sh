spark-submit --master yarn-client --num-executors 15 --executor-memory 28G --executor-cores 10 --conf spark.default.parallelism=500 --py-files config.py min.py
spark-submit --master yarn-client --num-executors 15 --executor-memory 28G --executor-cores 10 --conf spark.default.parallelism=500 --py-files config.py max.py
spark-submit --master yarn-client --num-executors 15 --executor-memory 28G --executor-cores 10 --conf spark.default.parallelism=500 --py-files config.py avg.py
spark-submit --master yarn-client --num-executors 15 --executor-memory 28G --executor-cores 10 --conf spark.default.parallelism=500 --py-files config.py sum_avg.py


spark-submit --master yarn-client --num-executors 15 --executor-memory 28G --executor-cores 10 --conf spark.default.parallelism=500 --py-files config.py count_all.py
spark-submit --master yarn-client --num-executors 15 --executor-memory 28G --executor-cores 10 --conf spark.default.parallelism=500 --py-files config.py count_row_num.py
spark-submit --master yarn-client --num-executors 15 --executor-memory 28G --executor-cores 10 --conf spark.default.parallelism=500 --py-files config.py count_day_pay_1.py
spark-submit --master yarn-client --num-executors 15 --executor-memory 28G --executor-cores 10 --conf spark.default.parallelism=500 --py-files config.py count_day_pay_2.py
spark-submit --master yarn-client --num-executors 15 --executor-memory 28G --executor-cores 10 --conf spark.default.parallelism=500 --py-files config.py count_day_open_avg_1.py
spark-submit --master yarn-client --num-executors 15 --executor-memory 28G --executor-cores 10 --conf spark.default.parallelism=500 --py-files config.py count_day_open_avg_2.py

spark-submit --master yarn-client --num-executors 15 --executor-memory 28G --executor-cores 10 --conf spark.default.parallelism=500 --py-files config.py third_count_1.py
spark-submit --master yarn-client --num-executors 15 --executor-memory 28G --executor-cores 10 --conf spark.default.parallelism=500 --py-files config.py third_count_2.py
spark-submit --master yarn-client --num-executors 15 --executor-memory 28G --executor-cores 10 --conf spark.default.parallelism=500 --py-files config.py third_count_3.py
spark-submit --master yarn-client --num-executors 15 --executor-memory 28G --executor-cores 10 --conf spark.default.parallelism=500 --py-files config.py third_2.py































