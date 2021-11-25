{{ config( materialized='table'  ) }}

with source_data as (

SELECT DISTINCT DWH_SCHADEN_ID  FROM (
SELECT DWH_SCHADEN_ID  FROM {{ref('vg_schaden_akt')}}  
UNION ALL
SELECT DWH_EREIGNIS_ID AS DWH_SCHADEN_ID FROM {{ref('vg_schadenereignis_akt')}} 
UNION ALL 
SELECT DWH_SCHADEN_ID FROM {{ref('vg_schadenereignis_bez_akt')}} 
UNION ALL 
SELECT DWH_SCHADEN_ID FROM {{ref('vg_schadenkategorie_akt')}} 
UNION ALL 
SELECT DWH_SCHADEN_ID FROM {{ref('vg_schadenzahlung_akt')}} 
UNION ALL
SELECT ID AS DWH_SCHADEN_ID FROM {{ref('vg_schadenrolle_akt')}}  
UNION ALL 
SELECT DWH_SCHADEN_ID AS DWH_SCHADEN_ID FROM {{ref('vg_schaden_art_akt')}}   
)
ORDER BY 1

					)

select *
from source_data