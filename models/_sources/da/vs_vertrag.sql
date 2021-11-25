select *
from {{ source('da', 'vs_vertrag') }}