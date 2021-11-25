select *
from {{ source('da', 'vg_mahnung_akt') }}