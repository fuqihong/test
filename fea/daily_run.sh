set -e

if [ $# -eq 1 ];then
  etl_day=$1
else
  etl_day=`date +%Y%m%d`
fi

nohup sh dailyYpdata.sh >> /var/log/pyspark/ypdata/dailyYpdata.log.${etl_day} 2>&1 &
