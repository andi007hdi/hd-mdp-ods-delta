select *
from {{ source('da', 'vg_vermittlervertrag_akt') }}