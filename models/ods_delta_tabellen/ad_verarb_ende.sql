{{ config( pre_hook =  "UPDATE INM_DATASHARING.AD_VERARB_ODS SET endezeit = CURRENT_TIMESTAMP, STATUS = 'done' WHERE ODS_LAUF_ID  = (SELECT max(ods_lauf_id) FROM INM_DATASHARING.AD_VERARB_ODS) AND PROZESSNAME ='2. Deltaermittlung'",
           post_hook = "DROP TABLE {{ this }}" ) }}

with source_data as (

select 1 as zaehler from {{ref('ad_anzahl_akt_verarbeitung') }} GROUP BY zaehler union all
select 1 as zaehler from {{ref('ad_chng_vertrag_status') }} GROUP BY zaehler union all
select 1 as zaehler from {{ref('ad_mod_partner') }} GROUP BY zaehler union all
select 1 as zaehler from {{ref('ad_mod_partner_coc') }} GROUP BY zaehler union all
select 1 as zaehler from {{ref('ad_mod_schaden') }} GROUP BY zaehler union all
select 1 as zaehler from {{ref('ad_mod_schaden_coc') }} GROUP BY zaehler union all
select 1 as zaehler from {{ref('ad_mod_vertrag') }} GROUP BY zaehler union all
select 1 as zaehler from {{ref('ad_mod_vertrag_coc') }} GROUP BY zaehler union all
select 1 as zaehler from {{ref('ad_verarb_ods') }} GROUP BY zaehler union all
select 1 as zaehler from {{ref('filter_vertrag_crm') }} GROUP BY zaehler

					)

select *
from source_data