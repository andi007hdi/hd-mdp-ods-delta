select *
from {{ source('da', 'vg_partner_kennzeichen_akt') }}