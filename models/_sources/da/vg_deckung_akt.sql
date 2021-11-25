select *
from {{ source('da', 'vg_deckung_akt') }}