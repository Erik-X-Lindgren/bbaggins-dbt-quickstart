SELECT DISTINCT
  DATE(fiscal_year) as fiscal_year
FROM {{ ref('fct_annual_financial_statements') }}
ORDER BY fiscal_year