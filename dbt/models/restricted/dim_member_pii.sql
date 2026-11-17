{{
    config(
        post_hook=[
            "{{ apply_masking_policy('first_name') }}",
            "{{ apply_masking_policy('last_name') }}",
            "{{ apply_masking_policy('ssn') }}",
            "{{ apply_masking_policy('street_address') }}",
            "{{ apply_masking_policy('birth_date', 'PII_DATE_MASK') }}"
        ] if target.type == 'snowflake' and env_var('APPLY_GOVERNANCE', 'false') == 'true' else []
    )
}}

{#
  Direct identifiers, kept apart from the analytics marts.

  Two layers of protection on Snowflake:
    1. Schema grants: only CLAIMS_PII_READER can see the RESTRICTED schema.
    2. Masking policies on each identifier column, so even a role that gets
       access by mistake sees masked values.
#}

select
    member_id,
    first_name,
    last_name,
    birth_date,
    ssn,
    street_address,
    city,
    state,
    zip_code
from {{ ref('stg_members') }}
