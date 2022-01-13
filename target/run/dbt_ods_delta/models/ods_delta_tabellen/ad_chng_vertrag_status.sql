

      create or replace transient table DWHHD_DEV.INM_DATASHARING.ad_chng_vertrag_status  as
      (with  __dbt__CTE__od_vertrag_akt as (
select *
from DWHHD_DEV.da.od_vertrag_akt
),  __dbt__CTE__od_vertragskontext_akt as (
select *
from DWHHD_DEV.da.od_vertragskontext_akt
),  __dbt__CTE__vs_vertrag as (
select *
from DWHHD_DEV.da.vs_vertrag
),source_data as (

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
		__dbt__CTE__od_vertrag_akt vert
	INNER JOIN __dbt__CTE__od_vertragskontext_akt vv
        ON
		vert.TIK_VERTRAG = vv.TIK_VERTRAG
		AND CURRENT_TIMESTAMP < vv.UNGUELTIGAB
		AND CURRENT_TIMESTAMP >= vv.GUELTIGVON
		AND vv.STATUS = 'STOR'
		AND vv.STATUSDATUM < CURRENT_TIMESTAMP
	INNER JOIN __dbt__CTE__vs_vertrag vertStornogrund
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
      );
    