with

source as (

    select * from {{ source('financial_data', 'annual_financial_statements') }}

)

select * from source