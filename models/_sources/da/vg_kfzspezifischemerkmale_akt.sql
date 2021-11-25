select *
from {{ source('da', 'vg_kfzspezifischemerkmale_akt') }}