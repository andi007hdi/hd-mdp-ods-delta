{{ config( materialized='table', unique_key="dwh_partner_id", 
post_hook = "ALTER TABLE {{ schema }}.AD_MOD_PARTNER ADD CONSTRAINT AD_MOD_PARTNER_PK PRIMARY KEY (DWH_PARTNER_ID)" ) }} 

with source_data as (

SELECT DISTINCT DWH_PARTNER_ID  FROM (
SELECT DWH_PARTNER_ID  FROM {{ref('vg_anschrift_akt')}}
UNION ALL
SELECT DWH_PARTNER_ID  FROM {{ref('vg_bankverbindung_akt')}}
UNION ALL 
SELECT DWH_PARTNER_ID FROM {{ref('vg_betriebswirtschaftdaten_akt')}}
UNION ALL 
SELECT PARTNER_ID AS DWH_PARTNER_ID FROM {{ref('vg_coc_auftrag_partner_akt')}}
UNION ALL 
SELECT DWH_PARTNER_ID FROM {{ref('vg_externereferenz_partner_akt')}}
UNION ALL 
SELECT DWH_PARTNER_ID FROM {{ref('vg_kommunikationskanal_akt')}}
UNION ALL 
SELECT DWH_KOSTENSTELLEN_ID AS DWH_PARTNER_ID FROM {{ref('vg_kostenstelle_akt')}}
UNION ALL 
SELECT DWH_PARTNER_ID FROM {{ref('vg_kostenstelle_zuordnung_akt')}} WHERE TYP = 'KST-MA'
UNION ALL 
SELECT DWH_KOSTENSTELLEN_ID AS DWH_PARTNER_ID FROM {{ref('vg_kostenstelle_zuordnung_akt')}} WHERE TYP = 'OE-KST'
UNION ALL 
SELECT DWH_PARTNER_ID FROM {{ref('vg_marketingmassnahme_akt')}} 
UNION ALL 
SELECT DWH_PARTNER_ID FROM {{ref('vg_partner_akt')}}
UNION ALL 
SELECT DWH_PARTNER_ID FROM {{ref('vg_partner_kennzeichen_akt')}}
UNION ALL 
SELECT DWH_PARTNER_ID FROM {{ref('vg_partnerbeziehung_akt')}}
UNION ALL 
SELECT DWH_PARTNER_ID FROM {{ref('vg_vermittlervereinbarung_akt')}}
UNION ALL 
SELECT DWH_PARTNER_ID FROM {{ref('vg_vermittlervertrag_akt')}}
UNION ALL 
SELECT DWH_PARTNER_ID FROM {{ref('vg_wirtschaftszweig_akt')}}
)
ORDER BY 1

					)

select *
from source_data