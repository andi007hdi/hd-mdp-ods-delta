select *
from {{ source('da', 'vg_betriebswirtschaftdaten_akt') }}