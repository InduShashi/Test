use warehouse ETL_XL_WH ;

copy into @admin.{{env_edl}}_chewy_bi_iceberg_data_export_stage/pet_profile/{{uuid}}/
from 
(select 
* from
cdm.pet_profile where TIME_CREATED between '{{interval_start_date}}' and '{{interval_end_date}}'
) 
-- partition by TIME_CREATED :: date :: string
  header = true;