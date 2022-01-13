with  __dbt__CTE__od_coc_auftrag_vertrag_akt as (
select *
from DWHHD_DEV.da.od_coc_auftrag_vertrag_akt
),source_data as (

SELECT
	TIK_VERTRAG
FROM
	DWHHD_DEV.INM_DATASHARING.ad_mod_vertrag 
WHERE
	TIK_VERTRAG NOT IN
     (
	SELECT
		TIK_VERTRAG
	FROM
		__dbt__CTE__od_coc_auftrag_vertrag_akt
	WHERE
		DT_COC_SPERRE <= current_timestamp
		OR AENDERUNGS_KZ = 'L'
    )

	)

select *
from source_data