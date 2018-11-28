#!/usr/bin/python
# encoding:utf-8

import sys
import datetime

reload(sys)
sys.setdefaultencoding('utf-8')

nowTime=datetime.date.today()
yes_time = str(nowTime - datetime.timedelta(days=1))

input_mid_table_name = 'dwd.fea_personal_cfsl_loan_deduct_seq_daily'

output_feature_hdfs_path = '/user/qihong.fu/feature-pool/fea_personal_cfsl_loan_daily/' + yes_time + '/'