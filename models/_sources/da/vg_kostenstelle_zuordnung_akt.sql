select *
from {{ source('da', 'vg_kostenstelle_zuordnung_akt') }}