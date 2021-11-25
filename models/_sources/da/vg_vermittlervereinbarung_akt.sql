select *
from {{ source('da', 'vg_vermittlervereinbarung_akt') }}