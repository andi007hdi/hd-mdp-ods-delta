select *
from {{ source('da', 'vg_vertragsteil_akt') }}