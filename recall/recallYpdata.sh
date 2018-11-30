if [ $# != 1 ] ; then
        echo "参数个数不正确"
        exit 1;
fi

spark-submit --master yarn-client --num-executors 15 --executor-memory 12G --executor-cores 4 --conf spark.default.parallelism=100 --py-files config.py match_sample_data.py $1
spark-submit --master yarn-client --num-executors 15 --executor-memory 12G --executor-cores 4 --conf spark.default.parallelism=300 --py-files config.py min.py  $1
spark-submit --master yarn-client --num-executors 15 --executor-memory 12G --executor-cores 4 --conf spark.default.parallelism=300 --py-files config.py max.py  $1
spark-submit --master yarn-client --num-executors 15 --executor-memory 12G --executor-cores 4 --conf spark.default.parallelism=300 --py-files config.py avg.py  $1 
spark-submit --master yarn-client --num-executors 15 --executor-memory 12G --executor-cores 4 --conf spark.default.parallelism=300 --py-files config.py sum_avg.py $1


spark-submit --master yarn-client --num-executors 15 --executor-memory 12G --executor-cores 4 --conf spark.default.parallelism=300 --py-files config.py count_all.py $1
spark-submit --master yarn-client --num-executors 15 --executor-memory 25G --executor-cores 4 --conf spark.default.parallelism=200 --py-files config.py count_row_num.py $1
spark-submit --master yarn-client --num-executors 15 --executor-memory 25G --executor-cores 4 --conf spark.default.parallelism=200 --py-files config.py count_day_open_avg_1.py $1
spark-submit --master yarn-client --num-executors 15 --executor-memory 25G --executor-cores 4 --conf spark.default.parallelism=200 --py-files config.py count_day_open_avg_2.py $1
spark-submit --master yarn-client --num-executors 15 --executor-memory 25G --executor-cores 4 --conf spark.default.parallelism=200 --py-files config.py count_day_pay_1.py $1
spark-submit --master yarn-client --num-executors 15 --executor-memory 25G --executor-cores 4 --conf spark.default.parallelism=200 --py-files config.py count_day_pay_2.py $1


spark-submit --master yarn-client --num-executors 15 --executor-memory 25G --executor-cores 4 --conf spark.default.parallelism=200 --py-files config.py third_count_1.py $1
spark-submit --master yarn-client --num-executors 15 --executor-memory 25G --executor-cores 4 --conf spark.default.parallelism=200 --py-files config.py third_count_2.py $1
spark-submit --master yarn-client --num-executors 15 --executor-memory 25G --executor-cores 4 --conf spark.default.parallelism=200 --py-files config.py third_count_3.py $1
spark-submit --master yarn-client --num-executors 15 --executor-memory 12G --executor-cores 4 --conf spark.default.parallelism=300 --py-files config.py third_2.py $1

 









