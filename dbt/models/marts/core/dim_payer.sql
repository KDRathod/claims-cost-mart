select
    payer_id,
    payer_name,
    payer_type,
    payer_type = 'government' as is_government_payer
from {{ ref('stg_payers') }}
