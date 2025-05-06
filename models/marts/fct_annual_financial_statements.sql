with unpivoted as (
    select
        company,
        account,
        year,
        nullif(value, '-') as value  -- value is always string; converts '-' to NULL
    from {{ ref('stg_annual_financial_statements') }}
    unpivot (
        value for year in (`2020`, `2021`, `2022`, `2023`, `2024`)
    )
    where lower(company) != 'company'
      and lower(account) != 'account'
),

numeric_pivot as (
    select
        company,
        year,
        max(case when account = 'Intäkter' then cast(value as float64) end) as revenue,
        max(case when account = 'Bruttoresultat' then cast(value as float64) end) as gross_profit,
        max(case when account = 'Rörelseresultat' then cast(value as float64) end) as operating_profit,
        max(case when account = 'Resultat efter finansiella poster' then cast(value as float64) end) as profit_after_financial_items,
        max(case when account = 'Nettoresultat' then cast(value as float64) end) as net_profit,
        max(case when account = 'Vinst per aktie' then cast(value as float64) end) as earnings_per_share,
        max(case when account = 'Utdelning' then cast(value as float64) end) as dividend,
        max(case when account = 'Materiella anläggningstillgångar' then cast(value as float64) end) as tangible_fixed_assets,
        max(case when account = 'Immateriella anläggningstillgångar' then cast(value as float64) end) as intangible_fixed_assets,
        max(case when account = 'Finansiella anläggningstillgångar' then cast(value as float64) end) as financial_fixed_assets,
        max(case when account = 'Varulager' then cast(value as float64) end) as inventory,
        max(case when account = 'Kundfordringar' then cast(value as float64) end) as accounts_receivable,
        max(case when account = 'Kortfristiga placeringar' then cast(value as float64) end) as short_term_investments,
        max(case when account = 'Kassa och bank' then cast(value as float64) end) as cash_and_bank,
        max(case when account = 'Övriga omsättningstillgångar' then cast(value as float64) end) as other_current_assets,
        max(case when account = 'Totala tillgångar' then cast(value as float64) end) as total_assets,
        max(case when account = 'Eget kapital - Moderbolagets ägare' then cast(value as float64) end) as equity_parent_companys_shareholders,
        max(case when account = 'Minoritetsintressen' then cast(value as float64) end) as minority_interests,
        max(case when account = 'Långfristiga skulder' then cast(value as float64) end) as long_term_liabilities,
        max(case when account = 'Kortfristiga skulder' then cast(value as float64) end) as short_term_liabilities,
        max(case when account = 'Totalt eget kapital och skulder' then cast(value as float64) end) as total_equity_and_liabilities
    from unpivoted
    where account not in (
        'Valuta', 
        'Startdatum Resultaträkning', 
        'Slutdatum Resultaträkning', 
        'Startdatum Balansräkning', 
        'Slutdatum Balansräkning'
    )
    group by company, year
),

meta_pivot as (
    select
        company,
        year,
        max(case when account = 'Valuta' then value end) as currency,
        max(case when account = 'Startdatum Resultaträkning' then value end) as income_statement_start_date,
        max(case when account = 'Slutdatum Resultaträkning' then value end) as income_statement_end_date,
        max(case when account = 'Startdatum Balansräkning' then value end) as balance_sheet_start_date,
        max(case when account = 'Slutdatum Balansräkning' then value end) as balance_sheet_end_date
    from unpivoted
    where account in (
        'Valuta', 
        'Startdatum Resultaträkning', 
        'Slutdatum Resultaträkning', 
        'Startdatum Balansräkning', 
        'Slutdatum Balansräkning'
    )
    group by company, year
)

select
    n.company,
    n.year as fiscal_year,
    n.revenue,
    n.gross_profit,
    n.operating_profit,
    n.profit_after_financial_items,
    n.net_profit,
    n.earnings_per_share,
    n.dividend,
    n.tangible_fixed_assets,
    n.intangible_fixed_assets,
    n.financial_fixed_assets,
    n.inventory,
    n.accounts_receivable,
    n.short_term_investments,
    n.cash_and_bank,
    n.other_current_assets,
    n.total_assets,
    n.equity_parent_companys_shareholders,
    n.minority_interests,
    n.long_term_liabilities,
    n.short_term_liabilities,
    n.total_equity_and_liabilities,
    m.currency,
    m.income_statement_start_date,
    m.income_statement_end_date,
    m.balance_sheet_start_date,
    m.balance_sheet_end_date
from numeric_pivot n
left join meta_pivot m
   on n.company = m.company
  and n.year = m.year
where n.company is not null