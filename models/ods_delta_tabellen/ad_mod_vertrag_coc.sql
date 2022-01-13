with source_data as (

SELECT
	TIK_VERTRAG
FROM
	{{ref('ad_mod_vertrag')}} 
WHERE
	TIK_VERTRAG NOT IN
     (
	SELECT
		TIK_VERTRAG
	FROM
		{{ref('od_coc_auftrag_vertrag_akt')}}
	WHERE
		DT_COC_SPERRE <= current_timestamp
		OR AENDERUNGS_KZ = 'L'
    )

	)

select *
from source_data