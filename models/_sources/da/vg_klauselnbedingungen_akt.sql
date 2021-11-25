select *
from {{ source('da', 'vg_klauselnbedingungen_akt') }}