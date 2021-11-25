select *
from {{ source('da', 'vg_vertragstandard_akt') }}