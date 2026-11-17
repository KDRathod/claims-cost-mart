with source as (
    select * from {{ source('raw', 'claim_lines') }}
),

deduped as (
    select *
    from source
    qualify row_number() over (
        partition by claim_id, line_number order by _loaded_at desc, _source_file desc
    ) = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['claim_id', 'line_number']) }} as claim_line_id,
    cast(claim_id as varchar)          as claim_id,
    cast(line_number as integer)       as line_number,
    cast(procedure_code as varchar)    as procedure_code,
    procedure_description,
    cast(units as integer)             as units,
    {{ money('charge_amount') }}       as charge_amount,
    _loaded_at
from deduped
