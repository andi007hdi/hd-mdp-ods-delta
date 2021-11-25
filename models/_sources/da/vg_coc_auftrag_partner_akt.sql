select *
from {{ source('da', 'vg_coc_auftrag_partner_akt') }}