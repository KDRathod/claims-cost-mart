{#
  The only model that reads direct identifiers. Everything downstream uses
  member_id plus de-identified attributes; the identifiers themselves are
  exposed only through restricted.dim_member_pii.
#}
select
    cast(member_id as varchar)   as member_id,
    first_name,
    last_name,
    cast(birth_date as date)     as birth_date,
    cast(death_date as date)     as death_date,
    ssn,
    upper(gender)                as gender,
    street_address,
    city,
    upper(state)                 as state,
    cast(zip_code as varchar)    as zip_code,
    county
from {{ source('raw', 'members') }}
