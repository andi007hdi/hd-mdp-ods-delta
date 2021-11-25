select *
from {{ source('da', 'vg_vertragsbeziehung_akt') }}