with stake_core as (
select 
    program_name
  , dt
  , lead(dt) over (order by dt) as lead_dt
  , amount_cumulative
  , daily_stake
  , daily_unstake
from (  
      select 
            PROGRAM_NAME
          , date_trunc('day', BLOCK_TIMESTAMP) as dt
          , sum(sum(case when action in ('MINT LOCK', 'UPDATE LOCK') then 1.00 
                         when action in ('EXIT') then -1.00 
                          else 0
                    end
             * coalesce(amount,0))) over (order by dt) as amount_cumulative
          , sum(case when action in ('MINT LOCK', 'UPDATE LOCK') then 1.00 
                         -- when action in ('EXIT') then -1.00 
                          else 0
                    end
             * coalesce(amount,0)) as daily_stake
          , sum(case --when action in ('MINT LOCK', 'UPDATE LOCK') then 1.00 
                          when action in ('EXIT') then -1.00 
                          else 0
                    end
             * coalesce(amount,0)) as daily_unstake
        
        
          -- , BLOCK_ID
          -- , TX_ID
          -- , SUCCEEDED
          -- , SIGNER
          -- , coalesce(LOCKER_ACCOUNT, lag(locker_account) ignore nulls over (partition by locker_nft order by block_timestamp)) as LOCKER_ACCOUNT
          -- , coalesce(LOCKER_NFT, lag(LOCKER_NFT) ignore nulls over (partition by locker_account order by block_timestamp)) as LOCKER_NFT
          -- , coalesce(MINT, lag(MINT) ignore nulls over (order by block_timestamp)) as mint
          -- , ACTION
          -- , AMOUNT
          -- , FACT_GOV_ACTIONS_ID
        
        from solana.gov.fact_gov_actions
        where program_name = 'marinade' and succeeded = true and mint = 'MNDEFzGvMt87ueuHvVU9VcTqsAP5b3fTGPsHuuPA5ey'
        group by 1,2
        order by dt desc
      )
)

, days AS (
    SELECT DATEADD(DAY, SEQ4(), '2022-04-01 00:00:00.000') AS dt
  FROM TABLE(GENERATOR(ROWCOUNT => 10000))
  WHERE DATEADD(DAY, SEQ4(), '2022-04-01 00:00:00.000') <= CURRENT_DATE
)

select 
    days.dt
  , coalesce(sc1.amount_cumulative, 0) as total_staked
  , coalesce(sc2.daily_stake,0) as daily_stake
  , coalesce(sc2.daily_unstake,0) as daily_unstake
from days
left join stake_core sc1 on sc1.dt <= days.dt and (sc1.lead_dt > days.dt or lead_dt is null)
left join stake_core sc2 on sc2.dt = days.dt 