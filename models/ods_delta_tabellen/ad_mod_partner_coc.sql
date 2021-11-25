{{ config( materialized='table'  ) }}

with source_data as (

SELECT
	DWH_PARTNER_ID
FROM
	{{ref('ad_mod_partner')}} 
WHERE
DWH_PARTNER_ID NOT IN
     (
SELECT
	TIK_PARTNER
FROM
	{{ref('od_coc_auftrag_partner_akt')}}
WHERE
	DT_COC_SPERRE <= CURRENT_TIMESTAMP
	OR AENDERUNGS_KZ = 'L'
    )

					)

select *
from source_data