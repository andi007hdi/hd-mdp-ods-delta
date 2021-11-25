select *
from {{ source('da', 'vg_selbstbehalte_akt') }}