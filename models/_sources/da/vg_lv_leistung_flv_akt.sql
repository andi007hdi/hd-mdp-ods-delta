select *
from {{ source('da', 'vg_lv_leistung_flv_akt') }}