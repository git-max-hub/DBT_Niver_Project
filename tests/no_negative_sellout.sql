select *
from {{ ref('stg_export_ppd_amazon') }}
where
    Sellout_Amount_1P_EUR < 0
    or Sellout_Amount_3P_EUR < 0
