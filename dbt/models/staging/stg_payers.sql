select
    cast(payer_id as varchar) as payer_id,
    payer_name,
    lower(payer_type)         as payer_type
from {{ source('raw', 'payers') }}
