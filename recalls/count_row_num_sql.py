# encoding:utf-8 
count_1_sql="""
sum( case when row_num<=1 then 1 else 0 end) as t01deizzz,
sum( case when row_num<=2 then 1 else 0 end) as t01dejzzz,
sum( case when row_num<=3 then 1 else 0 end) as t01dekzzz,
sum( case when row_num<=4 then 1 else 0 end) as t01delzzz 

"""
count_2_1_sql="""
sum( case when row_num<=1 and goods_if_subbizcatname = 'cf' then 1 else 0 end) as t01deizaz,
sum( case when row_num<=1 and goods_if_subbizcatname ='sl' then 1 else 0 end) as t01deizbz,
sum( case when row_num<=2 and goods_if_subbizcatname = 'cf' then 1 else 0 end) as t01dejzaz,
sum( case when row_num<=2 and goods_if_subbizcatname ='sl' then 1 else 0 end) as t01dejzbz,
sum( case when row_num<=3 and goods_if_subbizcatname = 'cf' then 1 else 0 end) as t01dekzaz,
sum( case when row_num<=3 and goods_if_subbizcatname ='sl' then 1 else 0 end) as t01dekzbz,
sum( case when row_num<=4 and goods_if_subbizcatname = 'cf' then 1 else 0 end) as t01delzaz,
sum( case when row_num<=4 and goods_if_subbizcatname ='sl' then 1 else 0 end) as t01delzbz

"""
count_2_2_sql="""
sum( case when row_num<=1 and pay_result =1 then 1 else 0 end) as t01deizzc,
sum( case when row_num<=2 and pay_result =1 then 1 else 0 end) as t01dejzzc,
sum( case when row_num<=3 and pay_result =1 then 1 else 0 end) as t01dekzzc,
sum( case when row_num<=4 and pay_result =1 then 1 else 0 end) as t01delzzc

"""
count_3_1_sql="""
sum( case when row_num<=1 and goods_if_subbizcatname = 'cf' and pay_result =1 then 1 else 0 end) as t01deizac,
sum( case when row_num<=1 and goods_if_subbizcatname ='sl' and pay_result =1 then 1 else 0 end) as t01deizbc,
sum( case when row_num<=2 and goods_if_subbizcatname = 'cf' and pay_result =1 then 1 else 0 end) as t01dejzac,
sum( case when row_num<=2 and goods_if_subbizcatname ='sl' and pay_result =1 then 1 else 0 end) as t01dejzbc,
sum( case when row_num<=3 and goods_if_subbizcatname = 'cf' and pay_result =1 then 1 else 0 end) as t01dekzac,
sum( case when row_num<=3 and goods_if_subbizcatname ='sl' and pay_result =1 then 1 else 0 end) as t01dekzbc,
sum( case when row_num<=4 and goods_if_subbizcatname = 'cf' and pay_result =1 then 1 else 0 end) as t01delzac,
sum( case when row_num<=4 and goods_if_subbizcatname ='sl' and pay_result =1 then 1 else 0 end) as t01delzbc

"""
count_3_2_sql="""
sum( case when row_num<=1 and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01deizza,
sum( case when row_num<=2 and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01dejzza,
sum( case when row_num<=2 and pay_result =0 and req_if_trademsg = 2 then 1 else 0 end) as t01dejzzb,
sum( case when row_num<=3 and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01dekzza,
sum( case when row_num<=3 and pay_result =0 and req_if_trademsg = 2 then 1 else 0 end) as t01dekzzb,
sum( case when row_num<=4 and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01delzza,
sum( case when row_num<=4 and pay_result =0 and req_if_trademsg = 2 then 1 else 0 end) as t01delzzb

"""
count_4_sql="""
sum( case when row_num<=1 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01deizba,
sum( case when row_num<=2 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01dejzba,
sum( case when row_num<=3 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01dekzaa,
sum( case when row_num<=3 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01dekzba,
sum( case when row_num<=4 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01delzaa,
sum( case when row_num<=4 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01delzba,
sum( case when row_num<=4 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 2 then 1 else 0 end) as t01delzbb

"""
