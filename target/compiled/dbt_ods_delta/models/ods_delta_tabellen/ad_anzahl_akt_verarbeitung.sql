-- depends_on: DWHHD_DEV.INM_DATASHARING.ad_verarb_ods


with source_data as (

SELECT t.tabelle, T.dml_flag,T.anzahl, CURRENT_TIMESTAMP AS  t_aend_dat
,
(SELECT max(ods_lauf_id) FROM INM_DATASHARING.AD_VERARB_ODS) AS ods_lauf_id
FROM (
SELECT 'VG_ANSCHRIFT_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_ANSCHRIFT_AKT group by dml_flag 
UNION
SELECT 'VG_BANKVERBINDUNG_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_BANKVERBINDUNG_AKT group by dml_flag
UNION
SELECT 'VG_BETRIEBSWIRTSCHAFTDATEN_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_BETRIEBSWIRTSCHAFTDATEN_AKT group by dml_flag
UNION
SELECT 'VG_COC_AUFTRAG_PARTNER_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_COC_AUFTRAG_PARTNER_AKT group by dml_flag 
UNION
SELECT 'VG_COC_AUFTRAG_VERTRAG_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_COC_AUFTRAG_VERTRAG_AKT group by dml_flag
UNION
SELECT 'VG_CODE_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_CODE_AKT group by dml_flag 
UNION
SELECT 'VG_EXTERNEREFERENZ_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_EXTERNEREFERENZ_AKT group by dml_flag
UNION
SELECT 'VG_EXTERNEREFERENZ_PARTNER_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_EXTERNEREFERENZ_PARTNER_AKT group by dml_flag 
UNION
SELECT 'VG_INTERNEREFERENZ_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_INTERNEREFERENZ_AKT group by dml_flag 
UNION
SELECT 'VG_KOMMUNIKATIONSKANAL_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_KOMMUNIKATIONSKANAL_AKT group by dml_flag
UNION
SELECT 'VG_KOSTENSTELLE_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_KOSTENSTELLE_AKT group by dml_flag 
UNION
SELECT 'VG_KOSTENSTELLE_ZUORDNUNG_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_KOSTENSTELLE_ZUORDNUNG_AKT group by dml_flag 
UNION
SELECT 'VG_LV_BEZUGSRECHT_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_LV_BEZUGSRECHT_AKT group by dml_flag
UNION
SELECT 'VG_LV_FOND_KURS_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_LV_FOND_KURS_AKT group by dml_flag 
UNION
SELECT 'VG_LV_FOND_STAMM_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_LV_FOND_STAMM_AKT group by dml_flag
UNION
SELECT 'VG_LV_FONDSPORTFOLIO_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_LV_FONDSPORTFOLIO_AKT group by dml_flag
UNION
SELECT 'VG_LV_LEISTUNG_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_LV_LEISTUNG_AKT group by dml_flag 
UNION
SELECT 'VG_LV_SCHWEBEGRUND_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_LV_SCHWEBEGRUND_AKT group by dml_flag
UNION
SELECT 'VG_LV_STICHTAGSWERTSATZ_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_LV_STICHTAGSWERTSATZ_AKT group by dml_flag 
UNION
SELECT 'VG_MAHNUNG_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_MAHNUNG_AKT group by dml_flag 
UNION
SELECT 'VG_MARKETINGMASSNAHME_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_MARKETINGMASSNAHME_AKT group by dml_flag
UNION
SELECT 'VG_PARTNER_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_PARTNER_AKT group by dml_flag
UNION
SELECT 'VG_PARTNER_KENNZEICHEN_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_PARTNER_KENNZEICHEN_AKT group by dml_flag 
UNION
SELECT 'VG_PARTNERBEZIEHUNG_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_PARTNERBEZIEHUNG_AKT group by dml_flag
UNION
SELECT 'VG_ROLLE_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_ROLLE_AKT group by dml_flag
UNION
SELECT 'VG_SCHADEN_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_SCHADEN_AKT group by dml_flag 
UNION
SELECT 'VG_SCHADEN_ART_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_SCHADEN_ART_AKT group by dml_flag
UNION
SELECT 'VG_SCHADENEREIGNIS_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_SCHADENEREIGNIS_AKT group by dml_flag
UNION
SELECT 'VG_SCHADENEREIGNIS_BEZ_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_SCHADENEREIGNIS_BEZ_AKT group by dml_flag 
UNION
SELECT 'VG_SCHADENKATEGORIE_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_SCHADENKATEGORIE_AKT group by dml_flag
UNION
SELECT 'VG_SCHADENROLLE_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_SCHADENROLLE_AKT group by dml_flag
UNION
SELECT 'VG_SCHADENZAHLUNG_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_SCHADENZAHLUNG_AKT group by dml_flag 
UNION
SELECT 'VG_VERMITTLERVEREINBARUNG_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_VERMITTLERVEREINBARUNG_AKT group by dml_flag
UNION
SELECT 'VG_VERMITTLERVERTRAG_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_VERMITTLERVERTRAG_AKT group by dml_flag
UNION
SELECT 'VG_VERTRAGSBEZIEHUNG_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_VERTRAGSBEZIEHUNG_AKT group by dml_flag 
UNION
SELECT 'VG_VERTRAGSKONTEXT_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_VERTRAGSKONTEXT_AKT group by dml_flag
UNION
SELECT 'VG_BEITRAG_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_BEITRAG_AKT group by dml_flag
UNION
SELECT 'VG_DRITTRECHTGLAEUBIGER_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_DRITTRECHTGLAEUBIGER_AKT group by dml_flag 
UNION
SELECT 'VG_FREMDVERTRAG_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_FREMDVERTRAG_AKT group by dml_flag
UNION
SELECT 'VG_KFZSPEZIFISCHEMERKMALE_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_KFZSPEZIFISCHEMERKMALE_AKT group by dml_flag
UNION
SELECT 'VG_KLAUSELNBEDINGUNGEN_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_KLAUSELNBEDINGUNGEN_AKT group by dml_flag 
UNION
SELECT 'VG_KONSORTE_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_KONSORTE_AKT group by dml_flag
UNION
SELECT 'VG_KRAFTFAHRZEUG_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_KRAFTFAHRZEUG_AKT group by dml_flag
UNION
SELECT 'VG_NUTZER_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_NUTZER_AKT group by dml_flag 
UNION
SELECT 'VG_SELBSTBEHALTE_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_SELBSTBEHALTE_AKT group by dml_flag
UNION
SELECT 'VG_STUFUNG_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_STUFUNG_AKT group by dml_flag
UNION
SELECT 'VG_VERSICHERTESOBJEKT_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_VERSICHERTESOBJEKT_AKT group by dml_flag 
UNION
SELECT 'VG_VERSICHERTESOBJEKTKFZ_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_VERSICHERTESOBJEKTKFZ_AKT group by dml_flag
UNION
SELECT 'VG_VERTRAG_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_VERTRAG_AKT group by dml_flag
UNION
SELECT 'VG_VERTRAGSTANDARD_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_VERTRAGSTANDARD_AKT group by dml_flag 
UNION
SELECT 'VG_VERTRAGSTEIL_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_VERTRAGSTEIL_AKT group by dml_flag
UNION
SELECT 'VG_VERTRAGSTEILBUENDEL_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_VERTRAGSTEILBUENDEL_AKT group by dml_flag
UNION
SELECT 'VG_ZUSCHL_NACHL_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_ZUSCHL_NACHL_AKT group by dml_flag 
UNION
SELECT 'VG_VVO_BLZ_ZESSIONARE_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_VVO_BLZ_ZESSIONARE_AKT group by dml_flag
UNION
SELECT 'VG_WERTUNGSGRUNDLTEILUNG_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_WERTUNGSGRUNDLTEILUNG_AKT group by dml_flag
UNION
SELECT 'VG_WIRTSCHAFTSZWEIG_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_WIRTSCHAFTSZWEIG_AKT group by dml_flag
UNION
SELECT 'VG_DECKUNG_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_DECKUNG_AKT group by dml_flag
UNION
SELECT 'VG_BONUSMALUS_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_BONUSMALUS_AKT group by dml_flag
UNION
SELECT 'VG_TYPREGIO_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_TYPREGIO_AKT group by dml_flag
UNION
SELECT 'VG_LV_LEISTUNG_FLV_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_LV_LEISTUNG_FLV_AKT group by dml_flag
UNION
SELECT 'VG_LV_LEISTUNG_KONV_AKT' AS TABELLE, dml_flag, count(*) as anzahl FROM DA.VG_LV_LEISTUNG_KONV_AKT group by dml_flag
)  T
ORDER BY 1, 2

					)

select *
from source_data