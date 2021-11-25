select *
from {{ source('da', 'vg_schadenkategorie_akt') }}