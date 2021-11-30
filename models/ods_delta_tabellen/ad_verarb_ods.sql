{{ config( materialized='incremental'  ) }}

with source_data as (

SELECT da.seq_ods.nextval AS ods_lauf_id, CURRENT_TIMESTAMP AS startzeit, NULL AS endezeit, CURRENT_TIMESTAMP AS lauf_dat, 'running' as status FROM DUAL

					)

select *
from source_data