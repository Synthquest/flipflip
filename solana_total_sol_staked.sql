with stake_base as (
  SELECT epoch, sum(active_stake) as sol_staked
  FROM solana.gov.fact_stake_accounts stk
  WHERE type_stake LIKE 'delegated'
    AND program LIKE 'stake'
    AND ACTIVATION_EPOCH <= EPOCH
    AND DEACTIVATION_EPOCH > epoch
    AND type_stake iLIKE 'delegated'
  group by epoch
)

SELECT 
  epoch, sol_staked
from stake_base
order by epoch desc


