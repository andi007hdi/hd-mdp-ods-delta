select *
from {{ source('da', 'od_schaden_akt') }}