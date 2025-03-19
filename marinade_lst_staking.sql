
with 
lst_table as (
  SELECT
  *
  FROM (
      values
        ('9eG63CdHjsfhHmobHgLtESGC8GabbmRcaSpHAZrtmhco', '4bZ6o3eUUNXhKuqjdCnCoPAoLgWiuLYixKaxoa8PpiKk', 'Du3Ysj1wKbxPKkuPPnvzQLQh8oMSVifs3jGZjJWXFmHN', 'mSOL Liquid')
        ,('ex9CfkBZZd6Nv9XdnoDmmB45ymbu4arXVk7g5pWnt3N', 'stWirqFCf2Uts1JBL1Jsd3r6VBWhgnpdPxCTe1MFjrq', '', 'mSOL Native')
        ,('noMa7dN4cHQLV4ZonXrC29HTKFpxrpFbDLK5Gub8W8t', 'exGQS9BR9Y1zLf36iM4pqHCDGoj2HChcjzu6kQTV2Ju', '', 'mSOL Native')
        ,('W1ZQRwUfSkDKy2oefRBUWph82Vr2zg9txWMA8RQazN5', 'W1ZQRwUfSkDKy2oefRBUWph82Vr2zg9txWMA8RQazN5', '3Kwv3pEAuoe4WevPB4rgMBTZndGDb53XT7qwQKnvHPfX','stSOL')
        ,('6iQKfEyhr3bZMotVkW6beNZz5CPAkiwvgV2CTje9pVSS', '6iQKfEyhr3bZMotVkW6beNZz5CPAkiwvgV2CTje9pVSS', 'BgKUXdS29YcHCFrPm5M8oLHiTzZaMDjsebggjoaQ6KFL', 'jito')
        ,('6WecYymEARvjG5ZyqkrVQ6YkhPfujNzWpSPwNKXHCbV2', '6WecYymEARvjG5ZyqkrVQ6YkhPfujNzWpSPwNKXHCbV2', 'rsrxDvYUXjH1RQj2Ke36LNZEVqGztATxFkqNukERqFT', 'bSOL')
        ,('HbJTxftxnXgpePCshA8FubsRj9MW4kfPscfuUfn44fnt', 'HbJTxftxnXgpePCshA8FubsRj9MW4kfPscfuUfn44fnt', 'AXu3DTw9AFq9FDTzX4vqA3XiT7LjrS5DpbsZaPpEx6qR', 'jSOL') --maybe
        ,('AzZRvyyMHBm8EHEksWxq4ozFL7JxLMydCDMGhqM6BVck', 'AzZRvyyMHBm8EHEksWxq4ozFL7JxLMydCDMGhqM6BVck', '', 'scnSOL') --could not find reserve, somewhere in the instructions here 5ocnV1qiCgaQR8Jb8xWnVbApfaygJ8tNoZfgPwsgx9kx
        ,('3b7XQeZ8nSMyjcQGTFJS5kBw4pXS2SqtB9ooHCnF2xV9', '3b7XQeZ8nSMyjcQGTFJS5kBw4pXS2SqtB9ooHCnF2xV9', 'AWDrV3Va8RKGMAo9bi5iYhqhETRsTT7NasRFC22uBj4A', 'mrgnLST')
  ) as v(w_authority, s_authority, reserve, lst)
)

, definitions as (
  select distinct authority, lst 
  from (
    select w_authority as authority, lst from lst_table
    union all
    select s_authority as authority, lst from lst_table
    union all
    select reserve as authority, lst from lst_table
  )
)


, raw_stake as (
  SELECT lst, stk.epoch, stk.active_stake, stk.active_stake - coalesce(lag(stk.active_stake) over (partition by stake_pubkey order by epoch),0) as delta
  , account_sol
  FROM solana.gov.fact_stake_accounts stk
  Inner Join definitions def on def.authority = stk.authorized_staker --and (def.lst = 'mSOL Native' or def.lst = 'mSOL Liquid')
  WHERE type_stake LIKE 'delegated'
    AND program LIKE 'stake'
    AND ACTIVATION_EPOCH <= EPOCH
    AND DEACTIVATION_EPOCH > epoch
    AND type_stake iLIKE 'delegated'
)

select 
    lst
  , epoch
  , sum(active_stake) as total_staked
  , sum(case when delta < 0 then delta else 0 end) as daily_stake
  , sum(case when delta > 0 then delta else 0 end) as daily_unstake
  , sum(delta) as daily_net
  , sum(account_sol) as tvl_sol

from raw_stake
group by 1,2
order by epoch desc, total_staked desc

