with source_data as (


select distinct tik_vertrag  from (
select tik_vertrag from {{ref('vg_coc_auftrag_vertrag_akt')}}
union all
select tik_vertrag from {{ref('vg_externereferenz_akt')}} 
union all
select tik_vertrag from {{ref('vg_internereferenz_akt')}}
union all
select tik_vertrag from {{ref('vg_lv_bezugsrecht_akt')}}
union all
select tik_vertrag from {{ref('vg_lv_leistung_akt')}} 
union all
select tik_vertrag from {{ref('vg_mahnung_akt')}} 
union all
select tik_vertrag from {{ref('vg_rolle_akt')}} 
union all
select tik_vertrag from {{ref('vg_vertrag_akt')}} 
union all
select tik_vertrag from {{ref('vg_vertragsbeziehung_akt')}} 
union all
select tik_vertrag from {{ref('vg_vertragskontext_akt')}} 
union all
select tik_vertrag from {{ref('vg_beitrag_akt')}}
union all
select tik_vertrag from {{ref('vg_bonusmalus_akt')}} 
union all
select tik_vertrag from {{ref('vg_deckung_akt')}}
union all
select tik_police as tik_vertrag from {{ref('vg_drittrechtglaeubiger_akt')}}
union all
select tik_vertrag from {{ref('vg_fremdvertrag_akt')}} 
union all
select tik_vertrag from {{ref('vg_kfzspezifischemerkmale_akt')}} 
union all
select tik_vertrag from {{ref('vg_klauselnbedingungen_akt')}}
union all
select tik_vertrag from {{ref('vg_kraftfahrzeug_akt')}} 
union all
select tik_vertrag from {{ref('vg_lv_leistung_flv_akt')}} 
union all
select tik_vertrag from {{ref('vg_lv_leistung_konv_akt')}} 
union all
select tik_vertrag from {{ref('vg_nutzer_akt')}} 
union all
select tik_vertrag from {{ref('vg_selbstbehalte_akt')}} 
union all
select tik_vertrag from {{ref('vg_stufung_akt')}}
union all
select tik_vertrag from {{ref('vg_typregio_akt')}}
union all
select tik_vertrag from {{ref('vg_versichertesobjekt_akt')}} 
union all
select tik_vertrag from {{ref('vg_versichertesobjektkfz_akt')}} 
union all
select tik_police as tik_vertrag from {{ref('vg_vertragstandard_akt')}}
union all
select tik_vertrag from {{ref('vg_vertragsteil_akt')}}
union all
select tik_vertrag from {{ref('vg_vertragsteilbuendel_akt')}} 
union all
select tik_vertrag from {{ref('vg_zuschl_nachl_akt')}} 
union all
select tik_vertrag from {{ref('vg_wertungsgrundlteilung_akt')}}
) 
ORDER BY 1

					)

select *
from source_data