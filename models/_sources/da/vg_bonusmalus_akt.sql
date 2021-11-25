select *
from {{ source('da', 'vg_bonusmalus_akt') }}