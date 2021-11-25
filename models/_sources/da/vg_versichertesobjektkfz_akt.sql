select *
from {{ source('da', 'vg_versichertesobjektkfz_akt') }}