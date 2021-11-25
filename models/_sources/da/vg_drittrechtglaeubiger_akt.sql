select *
from {{ source('da', 'vg_drittrechtglaeubiger_akt') }}