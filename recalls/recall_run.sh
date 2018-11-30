
if [ $# != 1 ] ; then
        echo "参数个数不正确"
        exit 1;
fi
nohup sh recallYpdata.sh $1 >> recallYpdata.log 2>&1 &