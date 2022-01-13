

      create or replace transient table DWHHD_DEV.INM_DATASHARING.ad_mod_vertrag  as
      (with  __dbt__CTE__vg_coc_auftrag_vertrag_akt as (
select *
from DWHHD_DEV.da.vg_coc_auftrag_vertrag_akt
),  __dbt__CTE__vg_externereferenz_akt as (
select *
from DWHHD_DEV.da.vg_externereferenz_akt
),  __dbt__CTE__vg_internereferenz_akt as (
select *
from DWHHD_DEV.da.vg_internereferenz_akt
),  __dbt__CTE__vg_lv_bezugsrecht_akt as (
select *
from DWHHD_DEV.da.vg_lv_bezugsrecht_akt
),  __dbt__CTE__vg_lv_leistung_akt as (
select *
from DWHHD_DEV.da.vg_lv_leistung_akt
),  __dbt__CTE__vg_mahnung_akt as (
select *
from DWHHD_DEV.da.vg_mahnung_akt
),  __dbt__CTE__vg_rolle_akt as (
select *
from DWHHD_DEV.da.vg_rolle_akt
),  __dbt__CTE__vg_vertrag_akt as (
select *
from DWHHD_DEV.da.vg_vertrag_akt
),  __dbt__CTE__vg_vertragsbeziehung_akt as (
select *
from DWHHD_DEV.da.vg_vertragsbeziehung_akt
),  __dbt__CTE__vg_vertragskontext_akt as (
select *
from DWHHD_DEV.da.vg_vertragskontext_akt
),  __dbt__CTE__vg_beitrag_akt as (
select *
from DWHHD_DEV.da.vg_beitrag_akt
),  __dbt__CTE__vg_bonusmalus_akt as (
select *
from DWHHD_DEV.da.vg_bonusmalus_akt
),  __dbt__CTE__vg_deckung_akt as (
select *
from DWHHD_DEV.da.vg_deckung_akt
),  __dbt__CTE__vg_drittrechtglaeubiger_akt as (
select *
from DWHHD_DEV.da.vg_drittrechtglaeubiger_akt
),  __dbt__CTE__vg_fremdvertrag_akt as (
select *
from DWHHD_DEV.da.vg_fremdvertrag_akt
),  __dbt__CTE__vg_kfzspezifischemerkmale_akt as (
select *
from DWHHD_DEV.da.vg_kfzspezifischemerkmale_akt
),  __dbt__CTE__vg_klauselnbedingungen_akt as (
select *
from DWHHD_DEV.da.vg_klauselnbedingungen_akt
),  __dbt__CTE__vg_kraftfahrzeug_akt as (
select *
from DWHHD_DEV.da.vg_kraftfahrzeug_akt
),  __dbt__CTE__vg_lv_leistung_flv_akt as (
select *
from DWHHD_DEV.da.vg_lv_leistung_flv_akt
),  __dbt__CTE__vg_lv_leistung_konv_akt as (
select *
from DWHHD_DEV.da.vg_lv_leistung_konv_akt
),  __dbt__CTE__vg_nutzer_akt as (
select *
from DWHHD_DEV.da.vg_nutzer_akt
),  __dbt__CTE__vg_selbstbehalte_akt as (
select *
from DWHHD_DEV.da.vg_selbstbehalte_akt
),  __dbt__CTE__vg_stufung_akt as (
select *
from DWHHD_DEV.da.vg_stufung_akt
),  __dbt__CTE__vg_typregio_akt as (
select *
from DWHHD_DEV.da.vg_typregio_akt
),  __dbt__CTE__vg_versichertesobjekt_akt as (
select *
from DWHHD_DEV.da.vg_versichertesobjekt_akt
),  __dbt__CTE__vg_versichertesobjektkfz_akt as (
select *
from DWHHD_DEV.da.vg_versichertesobjektkfz_akt
),  __dbt__CTE__vg_vertragstandard_akt as (
select *
from DWHHD_DEV.da.vg_vertragstandard_akt
),  __dbt__CTE__vg_vertragsteil_akt as (
select *
from DWHHD_DEV.da.vg_vertragsteil_akt
),  __dbt__CTE__vg_vertragsteilbuendel_akt as (
select *
from DWHHD_DEV.da.vg_vertragsteilbuendel_akt
),  __dbt__CTE__vg_zuschl_nachl_akt as (
select *
from DWHHD_DEV.da.vg_zuschl_nachl_akt
),  __dbt__CTE__vg_wertungsgrundlteilung_akt as (
select *
from DWHHD_DEV.da.vg_wertungsgrundlteilung_akt
),source_data as (


select distinct tik_vertrag  from (
select tik_vertrag from __dbt__CTE__vg_coc_auftrag_vertrag_akt
union all
select tik_vertrag from __dbt__CTE__vg_externereferenz_akt 
union all
select tik_vertrag from __dbt__CTE__vg_internereferenz_akt
union all
select tik_vertrag from __dbt__CTE__vg_lv_bezugsrecht_akt
union all
select tik_vertrag from __dbt__CTE__vg_lv_leistung_akt 
union all
select tik_vertrag from __dbt__CTE__vg_mahnung_akt 
union all
select tik_vertrag from __dbt__CTE__vg_rolle_akt 
union all
select tik_vertrag from __dbt__CTE__vg_vertrag_akt 
union all
select tik_vertrag from __dbt__CTE__vg_vertragsbeziehung_akt 
union all
select tik_vertrag from __dbt__CTE__vg_vertragskontext_akt 
union all
select tik_vertrag from __dbt__CTE__vg_beitrag_akt
union all
select tik_vertrag from __dbt__CTE__vg_bonusmalus_akt 
union all
select tik_vertrag from __dbt__CTE__vg_deckung_akt
union all
select tik_police as tik_vertrag from __dbt__CTE__vg_drittrechtglaeubiger_akt
union all
select tik_vertrag from __dbt__CTE__vg_fremdvertrag_akt 
union all
select tik_vertrag from __dbt__CTE__vg_kfzspezifischemerkmale_akt 
union all
select tik_vertrag from __dbt__CTE__vg_klauselnbedingungen_akt
union all
select tik_vertrag from __dbt__CTE__vg_kraftfahrzeug_akt 
union all
select tik_vertrag from __dbt__CTE__vg_lv_leistung_flv_akt 
union all
select tik_vertrag from __dbt__CTE__vg_lv_leistung_konv_akt 
union all
select tik_vertrag from __dbt__CTE__vg_nutzer_akt 
union all
select tik_vertrag from __dbt__CTE__vg_selbstbehalte_akt 
union all
select tik_vertrag from __dbt__CTE__vg_stufung_akt
union all
select tik_vertrag from __dbt__CTE__vg_typregio_akt
union all
select tik_vertrag from __dbt__CTE__vg_versichertesobjekt_akt 
union all
select tik_vertrag from __dbt__CTE__vg_versichertesobjektkfz_akt 
union all
select tik_police as tik_vertrag from __dbt__CTE__vg_vertragstandard_akt
union all
select tik_vertrag from __dbt__CTE__vg_vertragsteil_akt
union all
select tik_vertrag from __dbt__CTE__vg_vertragsteilbuendel_akt 
union all
select tik_vertrag from __dbt__CTE__vg_zuschl_nachl_akt 
union all
select tik_vertrag from __dbt__CTE__vg_wertungsgrundlteilung_akt
) 
ORDER BY 1

					)

select *
from source_data
      );
    