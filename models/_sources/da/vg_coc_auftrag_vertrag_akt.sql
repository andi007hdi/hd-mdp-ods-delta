select *
from {{ source('da', 'vg_coc_auftrag_vertrag_akt') }}