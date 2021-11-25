select *
from {{ source('da', 'vg_schadenzahlung_akt') }}