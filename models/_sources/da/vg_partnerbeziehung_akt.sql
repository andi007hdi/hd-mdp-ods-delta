select *
from {{ source('da', 'vg_partnerbeziehung_akt') }}