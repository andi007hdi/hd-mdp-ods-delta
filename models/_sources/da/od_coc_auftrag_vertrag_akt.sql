select *
from {{ source('da', 'od_coc_auftrag_vertrag_akt') }}