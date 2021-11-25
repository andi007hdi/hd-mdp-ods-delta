select *
from {{ source('da', 'vg_schadenereignis_bez_akt') }}