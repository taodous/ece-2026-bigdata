{{ config(severity='warn') }}

-- The first digit of a ZIP code identifies a region: it must match the state
select
    u.user_id,
    u.state,
    u.zip_code,
    r.zip_first_digit
from {{ ref('stg_users') }} u
join {{ ref('zip_regions') }} r on u.state = r.code
where left(u.zip_code, 1) <> cast(r.zip_first_digit as varchar)
