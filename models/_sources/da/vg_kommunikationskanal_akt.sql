select *
from {{ source('da', 'vg_kommunikationskanal_akt') }}