with  __dbt__CTE__od_schaden_akt as (
select *
from DWHHD_DEV.da.od_schaden_akt
),  __dbt__CTE__od_coc_auftrag_vertrag_akt as (
select *
from DWHHD_DEV.da.od_coc_auftrag_vertrag_akt
),source_data as (

SELECT
	ad.DWH_SCHADEN_ID
FROM
	DWHHD_DEV.INM_DATASHARING.ad_mod_schaden ad
INNER JOIN __dbt__CTE__od_schaden_akt s
ON
	ad.DWH_SCHADEN_ID = s.DWH_SCHADEN_ID
WHERE
	s.TIK_VERTRAG NOT IN
  (
	SELECT
		TIK_VERTRAG
	FROM
		__dbt__CTE__od_coc_auftrag_vertrag_akt 
	WHERE
		DT_COC_SPERRE <= current_timestamp
		OR AENDERUNGS_KZ = 'L'
  )
GROUP BY
	ad.DWH_SCHADEN_ID
ORDER BY
	1

					)

select *
from source_data