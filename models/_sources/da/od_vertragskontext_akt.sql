select *
from {{ source('da', 'od_vertragskontext_akt') }}