select
    date_trunc('day', block_timestamp) as dt
  , 'mSOL' as symbol
  --, platform
  , sum(coalesce(case when swap_from_symbol = 'MSOL' then swap_from_amount else swap_to_amount end,0)) as daily_volume
  , sum(coalesce(case when swap_from_symbol = 'MSOL' then 0 else swap_to_amount end,0)) as daily_buys
  , sum(coalesce(case when swap_from_symbol = 'MSOL' then swap_from_amount else 0 end,0)) as daily_sells
  , sum(coalesce(case when swap_from_symbol = 'MSOL' then swap_from_amount_usd else swap_to_amount_usd end,0)) as daily_volume_usd
  , sum(coalesce(case when swap_from_symbol = 'MSOL' then 0 else swap_to_amount_usd end,0)) as daily_buys_usd
  , sum(coalesce(case when swap_from_symbol = 'MSOL' then swap_from_amount_usd else 0 end,0)) as daily_sells_usd
  , count(distinct swapper) as total_wallets
  , median(case when swap_from_symbol = 'MSOL' then swap_from_amount_usd else swap_to_amount_usd end) AS median_daily_volume_usd
from solana.marinade.ez_swaps
where succeeded = true and (swap_from_symbol = 'MSOL' or swap_to_symbol = 'MSOL')
group by 1,2

union all

select
    date_trunc('day', block_timestamp) as dt
  , 'MNDE' as symbol
  --, platform
  , sum(coalesce(case when swap_from_symbol = 'MNDE' then swap_from_amount else swap_to_amount end,0)) as daily_volume
  , sum(coalesce(case when swap_from_symbol = 'MNDE' then 0 else swap_to_amount end,0)) as daily_buys
  , sum(coalesce(case when swap_from_symbol = 'MNDE' then swap_from_amount else 0 end,0)) as daily_sells
  , sum(coalesce(case when swap_from_symbol = 'MNDE' then swap_from_amount_usd else swap_to_amount_usd end,0)) as daily_volume_usd
  , sum(coalesce(case when swap_from_symbol = 'MNDE' then 0 else swap_to_amount_usd end,0)) as daily_buys_usd
  , sum(coalesce(case when swap_from_symbol = 'MNDE' then swap_from_amount_usd else 0 end,0)) as daily_sells_usd
  , count(distinct swapper) as total_wallets
  , median(case when swap_from_symbol = 'MNDE' then swap_from_amount_usd else swap_to_amount_usd end) AS median_daily_volume_usd  
from solana.marinade.ez_swaps
where succeeded = true and (swap_from_symbol = 'MNDE' or swap_to_symbol = 'MNDE')
group by 1,2
order by dt desc, symbol asc
