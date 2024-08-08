with cicstepAdata as
(select a.digitalLoanAccountId,
a.customerId,
a.loanAccountNumber,
b.crifApplicationId,
c.usefulHit,
a.flagDisbursement,
a.disbursementDateTime,
a.termsAndConditionsSubmitDateTime,
case when a.reloan_flag = 1 and a.loantype not like 'FLEXUP'then 'Reloan'
      when a.loantype = 'FLEXUP' and a.new_loan_type = 'Flex-up' and a.reloan_flag = 0 and a.flagDisbursement = 1 then 'Flex-up' 
              else a.new_loan_type end as LoanProduct,
row_number() over (partition by a.digitalLoanAccountId order by a.digitalLoanAccountId) rnk
FROM `risk_credit_mis.loan_master_table` a 
  JOIN `risk_credit_cic_data.cic_summary` b 
    ON a.digitalLoanAccountId = b.digitalLoanAccountId
    AND a.crifApplicationId = b.crifApplicationId
  JOIN (
              SELECT 
                a.digitalLoanAccountId
                , a.crifApplicationId
                , MAX(CASE WHEN DATE(b.ContractStartDate) < EXTRACT(DATE FROM a.termsAndConditionsSubmitDateTime) THEN 1 ELSE 0 END) as usefulHit
              FROM `risk_credit_mis.loan_master_table` a
              JOIN `risk_credit_cic_data.granted_contracts` b
              USING (digitalLoanAccountId, crifApplicationId)
              GROUP BY 1, 2
            ) c
    ON b.digitalLoanAccountId = c.digitalLoanAccountId
    AND b.crifApplicationId = c.crifApplicationId
    AND c.usefulHit = 1
where a.disbursementDateTime is not null
)
-- select digitalLoanAccountId, count(digitalLoanAccountId)cnt from cicstepAdata group by 1 having count(digitalLoanAccountId) > 1;
select * from cicstepAdata a
  LEFT JOIN (
    SELECT
        loanAccountNumber
        , SUM(CASE WHEN obs_min_inst_def30 >= 1 THEN 1 ELSE 0 END) as obsFPD30
        , SUM(CASE WHEN min_inst_def30 = 1 THEN 1 else 0 END) as defFPD30
        , sum(case when obs_min_inst_def30>=1 then (select max(disbursedloanamount) from `risk_credit_mis.loan_master_table` where loanAccountNumber = a1.loanAccountNumber) else 0 end) as obs_fpd30_vol
        , sum(case when min_inst_def30=1 then (select max(disbursedloanamount) from `risk_credit_mis.loan_master_table` where loanAccountNumber = a1.loanAccountNumber) else 0 end) as def_fpd30_vol
        , SUM(CASE WHEN obs_min_inst_def30 >= 2 THEN 1 ELSE 0 END) as obsFSPD30
        , SUM(CASE WHEN obs_min_inst_def30 >= 2 AND (min_inst_def30 = 2 or min_inst_def30 = 1) THEN 1 else 0 END) as defFSPD30
        , SUM(CASE WHEN obs_min_inst_def30 >= 2 THEN (select max(disbursedloanamount) from `risk_credit_mis.loan_master_table` where loanAccountNumber = a1.loanAccountNumber) ELSE 0 END) as obsFSPD30_vol
        , SUM(CASE WHEN obs_min_inst_def30 >= 2 AND (min_inst_def30 = 2 or min_inst_def30 = 1) THEN (select max(disbursedloanamount) from `risk_credit_mis.loan_master_table` where loanAccountNumber = a1.loanAccountNumber) else 0 END) as defFSPD30_vol
        , SUM(CASE WHEN obs_min_inst_def30 >= 3 THEN 1 ELSE 0 END) as obsFSTPD30
        , SUM(CASE WHEN obs_min_inst_def30 >= 3 AND (min_inst_def30 = 3 or min_inst_def30 = 2 or min_inst_def30 = 1) THEN 1 else 0 END) as defFSTPD30
        , SUM(CASE WHEN obs_min_inst_def30 >= 3 THEN (select max(disbursedloanamount) from `risk_credit_mis.loan_master_table` where loanAccountNumber = a1.loanAccountNumber) ELSE 0 END) as obsFSTPD30_vol
        , SUM(CASE WHEN obs_min_inst_def30 >= 3 AND (min_inst_def30 = 3 or min_inst_def30 = 2 or min_inst_def30 = 1) THEN (select max(disbursedloanamount) from `risk_credit_mis.loan_master_table` where loanAccountNumber = a1.loanAccountNumber) else 0 END) as defFSTPD30_vol
    FROM `risk_credit_mis.loan_deliquency_data` a1 
    GROUP BY 1
  ) d
ON a.loanAccountNumber = d.loanAccountNumber
  WHERE a.termsAndConditionsSubmitDateTime IS NOT NULL and a.rnk = 1