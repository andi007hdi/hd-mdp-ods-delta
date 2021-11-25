select *
from {{ source('da', 'vg_wirtschaftszweig_akt') }}