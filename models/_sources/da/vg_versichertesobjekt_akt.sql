select *
from {{ source('da', 'vg_versichertesobjekt_akt') }}