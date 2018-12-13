# encoding:utf-8 
pay_sql_1="""
sum(case when day_pay<=1 then 1 else 0 end) as t01deazzz,
sum(case when day_pay<=2 then 1 else 0 end) as t01debzzz,
sum(case when day_pay<=3 then 1 else 0 end) as t01deczzz,
sum(case when day_pay<=4 then 1 else 0 end) as t01dedzzz, 
sum(case when day_pay<=5 then 1 else 0 end) as t01deezzz,
sum(case when day_pay<=6 then 1 else 0 end) as t01defzzz,
sum(case when day_pay<=7 then 1 else 0 end) as t01degzzz,
sum(case when day_pay<=8 then 1 else 0 end) as t01dehzzz 

"""
pay_sql_2_1="""
sum(case when day_pay<=2 and goods_if_subbizcatname ='sl' then 1 else 0 end) as t01debzbz,
sum(case when day_pay<=3 and goods_if_subbizcatname = 'cf' then 1 else 0 end) as t01deczaz,
sum(case when day_pay<=3 and goods_if_subbizcatname ='sl' then 1 else 0 end) as t01deczbz,
sum(case when day_pay<=4 and goods_if_subbizcatname = 'cf' then 1 else 0 end) as t01dedzaz,
sum(case when day_pay<=4 and goods_if_subbizcatname ='sl' then 1 else 0 end) as t01dedzbz,
sum(case when day_pay<=5 and goods_if_subbizcatname = 'cf' then 1 else 0 end) as t01deezaz,
sum(case when day_pay<=5 and goods_if_subbizcatname ='sl' then 1 else 0 end) as t01deezbz,
sum(case when day_pay<=6 and goods_if_subbizcatname = 'cf' then 1 else 0 end) as t01defzaz,
sum(case when day_pay<=6 and goods_if_subbizcatname ='sl' then 1 else 0 end) as t01defzbz,
sum(case when day_pay<=7 and goods_if_subbizcatname = 'cf' then 1 else 0 end) as t01degzaz,
sum(case when day_pay<=7 and goods_if_subbizcatname ='sl' then 1 else 0 end) as t01degzbz,
sum(case when day_pay<=8 and goods_if_subbizcatname = 'cf' then 1 else 0 end) as t01dehzaz,
sum(case when day_pay<=8 and goods_if_subbizcatname ='sl' then 1 else 0 end) as t01dehzbz

"""
pay_sql_2_2="""
sum(case when day_pay<=1 and pay_result =1 then 1 else 0 end) as t01deazzc,
sum(case when day_pay<=2 and pay_result =1 then 1 else 0 end) as t01debzzc,
sum(case when day_pay<=3 and pay_result =1 then 1 else 0 end) as t01deczzc,
sum(case when day_pay<=4 and pay_result =1 then 1 else 0 end) as t01dedzzc,
sum(case when day_pay<=5 and pay_result =1 then 1 else 0 end) as t01deezzc,
sum(case when day_pay<=6 and pay_result =1 then 1 else 0 end) as t01defzzc,
sum(case when day_pay<=7 and pay_result =1 then 1 else 0 end) as t01degzzc,
sum(case when day_pay<=8 and pay_result =1 then 1 else 0 end) as t01dehzzc

"""
pay_sql_3_1="""
sum(case when day_pay<=2 and goods_if_subbizcatname ='sl' and pay_result =1 then 1 else 0 end) as t01debzbc,
sum(case when day_pay<=3 and goods_if_subbizcatname ='sl' and pay_result =1 then 1 else 0 end) as t01deczbc,
sum(case when day_pay<=4 and goods_if_subbizcatname = 'cf' and pay_result =1 then 1 else 0 end) as t01dedzac,
sum(case when day_pay<=4 and goods_if_subbizcatname ='sl' and pay_result =1 then 1 else 0 end) as t01dedzbc,
sum(case when day_pay<=5 and goods_if_subbizcatname = 'cf' and pay_result =1 then 1 else 0 end) as t01deezac,
sum(case when day_pay<=5 and goods_if_subbizcatname ='sl' and pay_result =1 then 1 else 0 end) as t01deezbc,
sum(case when day_pay<=6 and goods_if_subbizcatname = 'cf' and pay_result =1 then 1 else 0 end) as t01defzac,
sum(case when day_pay<=6 and goods_if_subbizcatname ='sl' and pay_result =1 then 1 else 0 end) as t01defzbc,
sum(case when day_pay<=7 and goods_if_subbizcatname = 'cf' and pay_result =1 then 1 else 0 end) as t01degzac,
sum(case when day_pay<=7 and goods_if_subbizcatname ='sl' and pay_result =1 then 1 else 0 end) as t01degzbc,
sum(case when day_pay<=8 and goods_if_subbizcatname = 'cf' and pay_result =1 then 1 else 0 end) as t01dehzac,
sum(case when day_pay<=8 and goods_if_subbizcatname ='sl' and pay_result =1 then 1 else 0 end) as t01dehzbc

"""
pay_sql_3_2="""
sum(case when day_pay<=2 and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01debzza,
sum(case when day_pay<=3 and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01deczza,
sum(case when day_pay<=4 and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01dedzza,
sum(case when day_pay<=5 and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01deezza,
sum(case when day_pay<=7 and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01degzza,
sum(case when day_pay<=6 and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01defzza,
sum(case when day_pay<=8 and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01dehzza,
sum(case when day_pay<=8 and pay_result =0 and req_if_trademsg = 2 then 1 else 0 end) as t01dehzzb

"""
pay_sql_4="""
sum(case when day_pay<=2 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01debzba,
sum(case when day_pay<=3 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01deczba,
sum(case when day_pay<=4 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01dedzba,
sum(case when day_pay<=5 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01deezaa,
sum(case when day_pay<=5 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01deezba,
sum(case when day_pay<=6 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01defzba,
sum(case when day_pay<=6 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01defzaa,
sum(case when day_pay<=7 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01degzaa,
sum(case when day_pay<=7 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01degzba,
sum(case when day_pay<=8 and goods_if_subbizcatname = 'cf' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01dehzaa,
sum(case when day_pay<=8 and goods_if_subbizcatname ='sl' and pay_result =0 and req_if_trademsg = 1 then 1 else 0 end) as t01dehzba,

"""
