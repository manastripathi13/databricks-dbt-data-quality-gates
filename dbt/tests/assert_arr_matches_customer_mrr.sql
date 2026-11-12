-- Reconciliation: on the latest day, total ARR in the product report equals 12 x current MRR in dim_customer.
with a as (select sum(arr_usd) as arr from {{ ref('rpt_arr_by_product') }}
           where as_of_date = (select max(as_of_date) from {{ ref('rpt_arr_by_product') }})),
     c as (select sum(current_arr_usd) as arr from {{ ref('dim_customer') }})
select * from a cross join c where abs(a.arr - c.arr) > 1
