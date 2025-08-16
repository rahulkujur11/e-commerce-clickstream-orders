
    
    

select
    sku as unique_field,
    count(*) as n_records

from "warehouse"."main_main"."dim_sku"
where sku is not null
group by sku
having count(*) > 1


