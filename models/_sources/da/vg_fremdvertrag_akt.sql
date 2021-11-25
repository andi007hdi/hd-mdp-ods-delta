select *
from {{ source('da', 'vg_fremdvertrag_akt') }}