select *
from {{ source('da', 'vg_schaden_akt') }}