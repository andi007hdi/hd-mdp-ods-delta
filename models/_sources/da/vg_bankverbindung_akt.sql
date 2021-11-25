select *
from {{ source('da', 'vg_bankverbindung_akt') }}