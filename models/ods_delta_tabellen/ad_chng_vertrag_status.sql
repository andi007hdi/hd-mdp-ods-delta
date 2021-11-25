{{ config( materialized='table' ) }}

with source_data as (

SELECT
	TIK_POLICE,
	TIK_VERTRAG,
	DWH_POLICE_ID,
	DWH_VERTRAG_ID,
	VERTRAGSTDOB,
	VERTRAGSOB,
	STATUSVERTRAG,
	ISTSTORNIERTIND,
	STORNOGRUND,
	STATUS,
	314 AS ODS_LAUF_ID,
	CURRENT_TIMESTAMP AS T_AEND_DAT
FROM
	(
	SELECT
		ROW_NUMBER() OVER (PARTITION BY vert.DWH_VERTRAG_ID,
		vert.VERTRAGSTDOB
	ORDER BY
		vertStornogrund.BIZPRCNR DESC) SORTER,
		vert.TIK_POLICE,
		vert.TIK_VERTRAG,
		vert.DWH_POLICE_ID,
		vert.DWH_VERTRAG_ID,
		vert.VERTRAGSTDOB,
		vert.VERTRAGSOB,
		vert.STATUSVERTRAG,
		vert.ISTSTORNIERTIND,
		vv.STATUS,
		vertStornogrund.STORNOGRUND
	FROM
		{{ref('od_vertrag_akt')}} vert
	INNER JOIN {{ref('od_vertragskontext_akt')}} vv
        ON
		vert.TIK_VERTRAG = vv.TIK_VERTRAG
		AND CURRENT_TIMESTAMP < vv.UNGUELTIGAB
		AND CURRENT_TIMESTAMP >= vv.GUELTIGVON
		AND vv.STATUS = 'STOR'
		AND vv.STATUSDATUM < CURRENT_TIMESTAMP
	INNER JOIN {{ref('vs_vertrag')}} vertStornogrund
        ON
		vert.DWH_VERTRAG_ID = vertStornogrund.DWH_VERTRAG_ID
		AND vertStornogrund.BIZPRCNR < vert.BIZPRCNR
		AND vertStornogrund.ISTSTORNIERTIND = 1
	WHERE
		vert.ISTSTORNIERTIND = 0
)
WHERE
	SORTER = 1
	AND TIK_POLICE IS NOT NULL
	AND TIK_VERTRAG IS NOT NULL

					)

select *
from source_data