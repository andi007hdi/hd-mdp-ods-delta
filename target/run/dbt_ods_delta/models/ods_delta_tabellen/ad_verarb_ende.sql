

      create or replace transient table DWHHD_DEV.INM_DATASHARING.ad_verarb_ende  as
      (

with source_data as (

select 1 as zaehler from DWHHD_DEV.INM_DATASHARING.ad_anzahl_akt_verarbeitung GROUP BY zaehler union all
select 1 as zaehler from DWHHD_DEV.INM_DATASHARING.ad_chng_vertrag_status GROUP BY zaehler union all
select 1 as zaehler from DWHHD_DEV.INM_DATASHARING.ad_mod_partner GROUP BY zaehler union all
select 1 as zaehler from DWHHD_DEV.INM_DATASHARING.ad_mod_partner_coc GROUP BY zaehler union all
select 1 as zaehler from DWHHD_DEV.INM_DATASHARING.ad_mod_schaden GROUP BY zaehler union all
select 1 as zaehler from DWHHD_DEV.INM_DATASHARING.ad_mod_schaden_coc GROUP BY zaehler union all
select 1 as zaehler from DWHHD_DEV.INM_DATASHARING.ad_mod_vertrag GROUP BY zaehler union all
select 1 as zaehler from DWHHD_DEV.INM_DATASHARING.ad_mod_vertrag_coc GROUP BY zaehler union all
select 1 as zaehler from DWHHD_DEV.INM_DATASHARING.ad_verarb_ods GROUP BY zaehler union all
select 1 as zaehler from DWHHD_DEV.INM_DATASHARING.filter_vertrag_crm GROUP BY zaehler

					)

select *
from source_data
      );
    