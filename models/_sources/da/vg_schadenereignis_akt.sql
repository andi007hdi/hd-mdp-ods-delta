select *
from {{ source('da', 'vg_schadenereignis_akt') }}