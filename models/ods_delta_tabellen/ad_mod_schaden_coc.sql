{{ config( materialized='table'  ) }}

with source_data as (

SELECT
	ad.DWH_SCHADEN_ID
FROM
	{{ref('ad_mod_schaden')}} ad
INNER JOIN {{ref('od_schaden_akt')}} s
ON
	ad.DWH_SCHADEN_ID = s.DWH_SCHADEN_ID
WHERE
	s.TIK_VERTRAG NOT IN
  (
	SELECT
		TIK_VERTRAG
	FROM
		{{ref('od_coc_auftrag_vertrag_akt')}} 
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