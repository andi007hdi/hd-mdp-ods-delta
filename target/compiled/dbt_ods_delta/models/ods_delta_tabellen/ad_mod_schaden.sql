

with  __dbt__CTE__vg_schaden_akt as (
select *
from DWHHD_DEV.da.vg_schaden_akt
),  __dbt__CTE__vg_schadenereignis_akt as (
select *
from DWHHD_DEV.da.vg_schadenereignis_akt
),  __dbt__CTE__vg_schadenereignis_bez_akt as (
select *
from DWHHD_DEV.da.vg_schadenereignis_bez_akt
),  __dbt__CTE__vg_schadenkategorie_akt as (
select *
from DWHHD_DEV.da.vg_schadenkategorie_akt
),  __dbt__CTE__vg_schadenzahlung_akt as (
select *
from DWHHD_DEV.da.vg_schadenzahlung_akt
),  __dbt__CTE__vg_schadenrolle_akt as (
select *
from DWHHD_DEV.da.vg_schadenrolle_akt
),  __dbt__CTE__vg_schaden_art_akt as (
select *
from DWHHD_DEV.da.vg_schaden_art_akt
),source_data as (

SELECT DISTINCT DWH_SCHADEN_ID  FROM (
SELECT DWH_SCHADEN_ID  FROM __dbt__CTE__vg_schaden_akt  
UNION ALL
SELECT DWH_EREIGNIS_ID AS DWH_SCHADEN_ID FROM __dbt__CTE__vg_schadenereignis_akt 
UNION ALL 
SELECT DWH_SCHADEN_ID FROM __dbt__CTE__vg_schadenereignis_bez_akt 
UNION ALL 
SELECT DWH_SCHADEN_ID FROM __dbt__CTE__vg_schadenkategorie_akt 
UNION ALL 
SELECT DWH_SCHADEN_ID FROM __dbt__CTE__vg_schadenzahlung_akt 
UNION ALL
SELECT ID AS DWH_SCHADEN_ID FROM __dbt__CTE__vg_schadenrolle_akt  
UNION ALL 
SELECT DWH_SCHADEN_ID AS DWH_SCHADEN_ID FROM __dbt__CTE__vg_schaden_art_akt   
)
ORDER BY 1

					)

select *
from source_data