

with unioned as (
    select * from {{ source('silver_raw', 'export_ppd_amazon_24_s2') }}
    union all
    select * from {{ source('silver_raw', 'export_ppd_amazon_25_s1') }}
),

base as (
    select
        try_cast([Year] as int) as [Year],
        try_cast([Month] as int) as [Month],
        [Country],
        [Universe],
        [retailer],
        [Axis],
        nullif(ltrim(rtrim([category])), '') as [category],
        [Sub_category],
        [market_definition],
        nullif(ltrim(rtrim([brand])), '') as [brand],
        [Product_code],
        [Product_description],

        -- cast numériques : on force '.' comme séparateur décimal
        try_cast(replace([Units_1P], ',', '.') as decimal(18,4)) as Units_1P,
        try_cast(replace([Units_3P], ',', '.') as decimal(18,4)) as Units_3P,

        try_cast(replace([Sellout_Amount_1P_EUR], ',', '.') as decimal(18,4)) as Sellout_Amount_1P_EUR,
        try_cast(replace([Sellout_Amount_3P_EUR], ',', '.') as decimal(18,4)) as Sellout_Amount_3P_EUR

    from unioned
)

select
    [Year],
    [Month],
    [Country],
    coalesce([Universe], '') as [Universe],
    [retailer],
    [Axis],
    coalesce([category], 'OTHER') as [category],
    [Sub_category],
    [market_definition],
    coalesce([brand], 'N/A') as [brand],
    [Product_code],
    [Product_description],
    Units_1P,
    Units_3P,
    Sellout_Amount_1P_EUR,
    Sellout_Amount_3P_EUR,
    right(cast([Year] as varchar(4)), 2) + right('00' + cast([Month] as varchar(2)), 2) as Year_Month,
    right('00' + cast([Month] as varchar(2)), 2) + '/' + right(cast([Year] as varchar(4)), 2) as [MM/YY],
    'EUROPE' as Region
from base
where coalesce([category], 'OTHER') not in ('OTHER', 'OTHER GIFTSETS', 'VIRTUAL BUNDLES')
