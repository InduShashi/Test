copy into @admin.{{env_edl}}_chewy_bi_iceberg_data_export_stage/pet_profile/{{uuid}}/
from 
(select 
* from
cdm.pet_profile where TIME_CREATED :: date >= current_date - {{process_days}}
) 
-- partition by TIME_CREATED :: date :: string
  header = true;