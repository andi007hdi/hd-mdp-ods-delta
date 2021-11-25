select *
from {{ source('da', 'od_coc_auftrag_partner_akt') }}