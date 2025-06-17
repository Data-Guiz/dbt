{% test is_valid_date(model, column_name) %}

with validation as (
    select
        {{ column_name }} as date_field
    from {{ model }}
),

validation_errors as (
    select
        date_field
    from validation
    where date_field is not null
        and not (
            date_field >= '1900-01-01'::date
            and date_field <= '2100-12-31'::date
        )
)

select *
from validation_errors

{% endtest %} 