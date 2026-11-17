{#
  De-identified member dimension. Safe for general analytics use:
  no name, no SSN, no street address, no full date of birth, ZIP cut to 3
  digits. The identifiers live in restricted.dim_member_pii.
#}

with members as (
    select * from {{ ref('stg_members') }}
),

as_of as (
    select {{ as_of_date() }} as as_of_date
)

select
    m.member_id,
    m.gender,
    extract(year from m.birth_date)                              as birth_year,
    {{ dbt.datediff('m.birth_date', 'a.as_of_date', 'year') }}   as age_years,
    case
        when {{ dbt.datediff('m.birth_date', 'a.as_of_date', 'year') }} < 18 then '0-17'
        when {{ dbt.datediff('m.birth_date', 'a.as_of_date', 'year') }} < 35 then '18-34'
        when {{ dbt.datediff('m.birth_date', 'a.as_of_date', 'year') }} < 50 then '35-49'
        when {{ dbt.datediff('m.birth_date', 'a.as_of_date', 'year') }} < 65 then '50-64'
        else '65+'
    end                                                          as age_band,
    substr(m.zip_code, 1, 3)                                     as zip3,
    m.county,
    m.state,
    m.death_date is not null                                     as is_deceased
from members m
cross join as_of a
