select *
from {{ source('da', 'od_vertrag_akt') }}