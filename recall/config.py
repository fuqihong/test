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


input_sample_path_pre = '/user/tianchuang/spark/recall/'

output_sample_data_path_pre = '/user/qihong.fu/feature-pool/recall/'

output_feature_path_pre = '/user/qihong.fu/feature-pool/fea_personal_cfsl_loan_daily/' 
