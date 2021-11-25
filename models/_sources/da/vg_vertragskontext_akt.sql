select *
from {{ source('da', 'vg_vertragskontext_akt') }}