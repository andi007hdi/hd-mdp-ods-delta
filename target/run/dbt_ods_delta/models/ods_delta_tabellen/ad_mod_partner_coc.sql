

      create or replace transient table DWHHD_DEV.INM_DATASHARING.ad_mod_partner_coc  as
      (with  __dbt__CTE__od_coc_auftrag_partner_akt as (
select *
from DWHHD_DEV.da.od_coc_auftrag_partner_akt
),source_data as (

SELECT
	DWH_PARTNER_ID
FROM
	DWHHD_DEV.INM_DATASHARING.ad_mod_partner 
WHERE
DWH_PARTNER_ID NOT IN
     (
SELECT
	TIK_PARTNER
FROM
	__dbt__CTE__od_coc_auftrag_partner_akt
WHERE
	DT_COC_SPERRE <= CURRENT_TIMESTAMP
	OR AENDERUNGS_KZ = 'L'
    )

					)

select *
from source_data
      );
    