#!/usr/bin/python
# encoding:utf-8

import sys
import datetime

reload(sys)
sys.setdefaultencoding('utf-8')


def dropFrame(rows):
    x = ''
    for s in rows:
        x += str(s) + ','
    x = x[:-1]
    return x



nowTime=datetime.date.today()
yes_time = str(nowTime - datetime.timedelta(days=1))
#yes_time = '2018-11-02'
input_mid_table_name = "dwd.fea_personal_cfsl_loan_deduct_seq_daily where dt <= '{yes_time}' and idcard is not null ".format(yes_time = yes_time)

output_feature_hdfs_path = '/user/qihong.fu/feature-pool/fea_personal_cfsl_loan_daily/' + yes_time + '/'
