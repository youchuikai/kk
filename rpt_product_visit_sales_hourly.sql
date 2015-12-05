use rpt_web;
drop table temp.tmp_rpt_product_visit_sales_hourly_1 ;
create table temp.tmp_rpt_product_visit_sales_hourly_1 as 
SELECT DISTINCT soi_uid old_uid , 1 as old_user_status
FROM   ods.xd_js_sorder_info 
WHERE  date<'{$date}';

drop table temp.tmp_rpt_product_visit_sales_hourly_2 ;
create table temp.tmp_rpt_product_visit_sales_hourly_2 as 
SELECT platform_id,
(case when terminal_id= 1 then 1 when terminal_id= 2 then 4 when terminal_id= 3 then 2 when terminal_id in(4,5) then 3 when terminal_id= 6 then 5 end) terminal_id,
product_id,
site_id,
sum(sale_total_quantity) sale_total_quantity,
sum(sale_total_amount) sale_total_amount,
sum(reject_total_quantity) reject_total_quantity,
sum(order_count) order_count,
sum(paid_order_count) paid_order_count,
sum(product_amount) product_amount,
sum(price_per_unit) price_per_unit,
sum(price_per_customer) price_per_customer,
sum(buy_users) buy_users,
sum(new_buy_user_count) new_buy_user_count,
sum(paid_user_count) paid_user_count,
sum(skc_count) skc_count,
sum(sku_count) sku_count,
sum(uv) uv,
sum(pv) pv,
X.hour
FROM 
(
SELECT
a.terminal_id terminal_id,
a.platform_id platform_id,
b.sg_goods_id product_id, 
a.site_id site_id,
sum( b.sg_goods_nums ) sale_total_quantity,
sum( b.sg_real_price) sale_total_amount,
sum(case when e.sr_status=1 then b.sg_goods_nums else 0 end) reject_total_quantity,
0 order_count,
count(distinct nvl(f.main_order_no,a.soi_no)) paid_order_count,
0 product_amount,
nvl(sum(b.sg_real_price)/sum(b.sg_goods_nums),0) price_per_unit,
nvl(sum(b.sg_real_price)/count(distinct b.sg_uid ),0)  price_per_customer,
0 buy_users,
count(distinct (case when d.old_user_status is null then b.sg_uid end)) new_buy_user_count,
count(distinct b.sg_uid ) paid_user_count,
count(distinct  (CASE WHEN b.sg_goods_zid = 0 THEN b.sg_goods_fid ELSE b.sg_goods_zid END)  ) skc_count,
count(distinct concat((CASE WHEN b.sg_goods_zid = 0 THEN b.sg_goods_fid ELSE b.sg_goods_zid END),'\\;',b.sg_goods_fid) ) sku_count,
0 uv,
0 pv,
a.hour
FROM (
SELECT
soi_no, 
soi_uid,
soi_is_main,
hour(soi_pay_time) hour,
(case when soi_pay_from in(3,6) then 3 when soi_pay_from in(4,5,7,8) then 4 when soi_pay_from in(9) then 6 else soi_pay_from end) terminal_id, 
(case when soi_pay_from in(1,2,3,4,5,9) then 1 when soi_pay_from in(6,7,8) then 2 end) site_id, 
(CASE soi_pay_from WHEN 1 THEN 1 ELSE 2 end) platform_id  
FROM   ods.xd_js_sorder_info
WHERE  DATE='{$date}' 
) a 
INNER JOIN
(SELECT  sg_order_no,sg_goods_id,sg_goods_nums,sg_real_price,sg_uid,sg_goods_zid,sg_goods_fid,sg_pay_status  
FROM  ods.xd_js_sorder_goods          where DATE='{$date}'
) b
ON a.soi_no = b.sg_order_no 
LEFT JOIN temp.tmp_rpt_product_visit_sales_hourly_1 d 
ON a.soi_uid = d.old_uid 
LEFT JOIN ods.xd_js_sorder_refund e
ON b.sg_order_no = e.sr_order_no
LEFT JOIN ods.xd_js_sorder_relation f
ON a.soi_no = f.sub_order_no
GROUP BY  a.platform_id,a.terminal_id,b.sg_goods_id,a.site_id,a.hour
union all

SELECT a.terminal_id terminal_id,
a.platform_id platform_id,
b.sg_goods_id product_id,   
a.site_id site_id,
0 sale_total_quantity,
0 sale_total_amount,
0 reject_total_quantity,
count(distinct nvl(c.main_order_no,a.soi_no)) order_count,  
0 paid_order_count,
sum( b.sg_goods_nums)  product_amount,  
0 price_per_unit,
0 price_per_customer,
count(distinct  b.sg_uid) buy_users,
0 new_buy_user_count,
0 paid_user_count,
0 skc_count,
0 sku_count,
0 uv,
0 pv,
a.hour
FROM (SELECT soi_no,
soi_uid,
soi_is_main,
hour(soi_create_time) hour,
(case when soi_from in(3,6) then 3 when soi_from in(4,5,7,8) then 4 when soi_from in(9) then 6 else soi_from end) terminal_id, 
(case when soi_from in(1,2,3,4,5,9) then 1 when soi_from in(6,7,8) then 2 end) site_id, 
(CASE soi_from WHEN 1 THEN 1 ELSE 2 end) platform_id  
FROM   ods.xd_js_sorder_info_create
WHERE  DATE='{$date}' 
) a 

INNER JOIN
(SELECT  sg_order_no,sg_goods_id,sg_goods_nums,sg_real_price,sg_uid,sg_goods_zid,sg_goods_fid,sg_pay_status  
FROM  ods.xd_js_sorder_goods_create          where DATE='{$date}'
) b
ON a.soi_no = b.sg_order_no
LEFT JOIN ods.xd_js_sorder_relation c
ON a.soi_no = c.sub_order_no 
GROUP BY  a.platform_id,a.terminal_id,b.sg_goods_id,a.site_id,a.hour
UNION ALL     
SELECT terminal_id,
platform_id,
product_id,
site_id,
0 sale_total_quantity,
0 sale_total_amount,
0 reject_total_quantity,
0 order_count,
0 paid_order_count,
0 product_amount,
0 price_per_unit,
0 price_per_customer,
0 buy_users,
0 new_buy_user_count,
0 paid_user_count,
0 skc_count,
0 sku_count,
uv_count uv,
pv_count pv,
hour 
FROM rpt_web.fact_product_visit_hour where date='{$date}'
UNION ALL
SELECT terminal_id,
platform_id,
product_id,
site_id,
0 sale_total_quantity,
0 sale_total_amount,
0 reject_total_quantity,
0 order_count,
0 paid_order_count,
0 product_amount,
0 price_per_unit,
0 price_per_customer,
0 buy_users,
0 new_buy_user_count,
0 paid_user_count,
0 skc_count,
0 sku_count,
uv_count uv,
pv_count pv,
hour
FROM rpt_mobile.rpt_product_visit_hour where date='{$date}'
) X
group by   platform_id,(case when terminal_id= 1 then 1 when terminal_id= 2 then 4 when terminal_id= 3 then 2 when terminal_id in(4,5) then 3 when terminal_id= 6 then 5 end), product_id, site_id,X.hour;


alter table rpt_product_visit_sales_hourly drop partition(date='{$date}');
INSERT OVERWRITE  TABLE rpt_web.rpt_product_visit_sales_hourly
PARTITION (date = '{$date}')  
SELECT 
B.platform_id,
E.platform platform_name,
B.terminal_id,
null terminal_name,
A.brand_id,
G.brand_name,
A.shop_id,
H.shop_name,
0 bag_id, 
0 bag_name, 
A.product_id,
I.product_name,
nvl(B.sale_total_quantity,0) sale_total_quantity,
nvl(B.sale_total_amount,0) sale_total_amount,
0 discount_sale_total_quantity,
0 discount_sale_total_amount,
nvl(B.reject_total_quantity,0) reject_total_quantity,
0 discount_reject_total_quantity,
0 cash_coupon,
nvl(B.order_count,0) order_count,
nvl(B.paid_order_count,0) paid_order_count,
nvl(B.product_amount,0) product_amount,
nvl(B.price_per_unit,0) price_per_unit,
nvl(B.price_per_customer,0) price_per_customer,
nvl(B.buy_users,0) buy_users,
nvl(B.new_buy_user_count,0) new_buy_user_count,
nvl(B.uv,0)  uv,
nvl(B.pv,0)  pv,
nvl(B.paid_user_count,0) paid_user_count,
nvl(B.skc_count,0) skc_count,
nvl(B.sku_count,0) sku_count,
B.hour,
B.site_id,
(case when A.shop_id=0 then 1 when A.shop_id>0 then 2 end) goods_type_id
FROM  (SELECT si_brand_id brand_id,si_gs_id shop_id,si_id product_id from ods.xd_js_sgoods_info where from_unixtime( cast(substring(si_start_time,0,10) as bigint),'yyyy-MM-dd')<='{$date}' and from_unixtime( cast(substring(si_end_time,0,10) as bigint),'yyyy-MM-dd')>='{$date}') A
LEFT JOIN  temp.tmp_rpt_product_visit_sales_hourly_2 B  ON A.product_id=B.product_id
LEFT JOIN ods.dim_platform E on B.platform_id=E.platform_id
LEFT JOIN ods.dim_brand    G on A.brand_id   =G.brand_id
LEFT JOIN ods.dim_shop     H on A.shop_id    =H.shop_id
LEFT JOIN ods.dim_product  I on A.product_id =I.product_id;