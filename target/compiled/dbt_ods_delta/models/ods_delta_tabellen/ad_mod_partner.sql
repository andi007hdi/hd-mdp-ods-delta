with  __dbt__CTE__vg_anschrift_akt as (
select *
from DWHHD_DEV.da.vg_anschrift_akt
),  __dbt__CTE__vg_bankverbindung_akt as (
select *
from DWHHD_DEV.da.vg_bankverbindung_akt
),  __dbt__CTE__vg_betriebswirtschaftdaten_akt as (
select *
from DWHHD_DEV.da.vg_betriebswirtschaftdaten_akt
),  __dbt__CTE__vg_coc_auftrag_partner_akt as (
select *
from DWHHD_DEV.da.vg_coc_auftrag_partner_akt
),  __dbt__CTE__vg_externereferenz_partner_akt as (
select *
from DWHHD_DEV.da.vg_externereferenz_partner_akt
),  __dbt__CTE__vg_kommunikationskanal_akt as (
select *
from DWHHD_DEV.da.vg_kommunikationskanal_akt
),  __dbt__CTE__vg_kostenstelle_akt as (
select *
from DWHHD_DEV.da.vg_kostenstelle_akt
),  __dbt__CTE__vg_kostenstelle_zuordnung_akt as (
select *
from DWHHD_DEV.da.vg_kostenstelle_zuordnung_akt
),  __dbt__CTE__vg_marketingmassnahme_akt as (
select *
from DWHHD_DEV.da.vg_marketingmassnahme_akt
),  __dbt__CTE__vg_partner_akt as (
select *
from DWHHD_DEV.da.vg_partner_akt
),  __dbt__CTE__vg_partner_kennzeichen_akt as (
select *
from DWHHD_DEV.da.vg_partner_kennzeichen_akt
),  __dbt__CTE__vg_partnerbeziehung_akt as (
select *
from DWHHD_DEV.da.vg_partnerbeziehung_akt
),  __dbt__CTE__vg_vermittlervereinbarung_akt as (
select *
from DWHHD_DEV.da.vg_vermittlervereinbarung_akt
),  __dbt__CTE__vg_vermittlervertrag_akt as (
select *
from DWHHD_DEV.da.vg_vermittlervertrag_akt
),  __dbt__CTE__vg_wirtschaftszweig_akt as (
select *
from DWHHD_DEV.da.vg_wirtschaftszweig_akt
),source_data as (

SELECT DISTINCT DWH_PARTNER_ID  FROM (
SELECT DWH_PARTNER_ID  FROM __dbt__CTE__vg_anschrift_akt
UNION ALL
SELECT DWH_PARTNER_ID  FROM __dbt__CTE__vg_bankverbindung_akt
UNION ALL 
SELECT DWH_PARTNER_ID FROM __dbt__CTE__vg_betriebswirtschaftdaten_akt
UNION ALL 
SELECT PARTNER_ID AS DWH_PARTNER_ID FROM __dbt__CTE__vg_coc_auftrag_partner_akt
UNION ALL 
SELECT DWH_PARTNER_ID FROM __dbt__CTE__vg_externereferenz_partner_akt
UNION ALL 
SELECT DWH_PARTNER_ID FROM __dbt__CTE__vg_kommunikationskanal_akt
UNION ALL 
SELECT DWH_KOSTENSTELLEN_ID AS DWH_PARTNER_ID FROM __dbt__CTE__vg_kostenstelle_akt
UNION ALL 
SELECT DWH_PARTNER_ID FROM __dbt__CTE__vg_kostenstelle_zuordnung_akt WHERE TYP = 'KST-MA'
UNION ALL 
SELECT DWH_KOSTENSTELLEN_ID AS DWH_PARTNER_ID FROM __dbt__CTE__vg_kostenstelle_zuordnung_akt WHERE TYP = 'OE-KST'
UNION ALL 
SELECT DWH_PARTNER_ID FROM __dbt__CTE__vg_marketingmassnahme_akt 
UNION ALL 
SELECT DWH_PARTNER_ID FROM __dbt__CTE__vg_partner_akt
UNION ALL 
SELECT DWH_PARTNER_ID FROM __dbt__CTE__vg_partner_kennzeichen_akt
UNION ALL 
SELECT DWH_PARTNER_ID FROM __dbt__CTE__vg_partnerbeziehung_akt
UNION ALL 
SELECT DWH_PARTNER_ID FROM __dbt__CTE__vg_vermittlervereinbarung_akt
UNION ALL 
SELECT DWH_PARTNER_ID FROM __dbt__CTE__vg_vermittlervertrag_akt
UNION ALL 
SELECT DWH_PARTNER_ID FROM __dbt__CTE__vg_wirtschaftszweig_akt
)
ORDER BY 1

					)

select *
from source_data