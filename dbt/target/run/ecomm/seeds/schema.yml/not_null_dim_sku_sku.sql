
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sku
from "warehouse"."main_main"."dim_sku"
where sku is null



  
  
      
    ) dbt_internal_test