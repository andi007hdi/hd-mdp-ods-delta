select *
from {{ source('da', 'vg_schaden_art_akt') }}