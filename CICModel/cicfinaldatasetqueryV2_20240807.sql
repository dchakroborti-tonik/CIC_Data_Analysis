WITH
  CICBaseTable AS ----To create combine CIC raw data combining Granted and Non Granted table
  ( -- Query FOR dfgranted
  SELECT
    digitalLoanAccountId,
    crifApplicationId,
    customerId,
    processEngineGuid,
    requestGuid,
    ContractHistoryType,
    CBContractCode,
    ContractEndDate,
    ContractPhase,
    ContractPhaseDesc,
    ContractStartDate,
    ContractStatus,
    ContractStatusDesc,
    ContractType,
    ContractTypeDesc,
    Currency,
    CurrencyDesc,
    LastUpdateDate,
    OriginalCurrency,
    OriginalCurrencyDesc,
    ProviderCodeEncrypted,
    ProviderContractNo,
    ReferenceNo,
    Role,
    RoleDesc,
    BilledAmount,
    BoardResolutionFlag,
    BoardResolutionFlagDesc,
    CancellationDate,
    CardReferenceCode,
    ChargedAmount,
    CreditLimit,
    CreditPurpose,
    CreditPurposeDesc,
    FinancedAmount,
    FirstPaymentDate,
    FlagCardUsed,
    HolderLiability,
    HolderLiabilityDesc,
    InstallmentType,
    InstallmentTypeDesc,
    InstallmentsNumber,
    LastChargeDate,
    LastPaymentAmount,
    LastPaymentDate,
    MinPaymentIndicator,
    MinPaymentIndicatorDesc,
    MinPaymentPercentage,
    MonthlyPaymentAmount,
    NextPayment,
    NextPaymentDate,
    OutstandingBalance,
    OutstandingBalanceUnbilled,
    OutstandingPaymentsNumber,
    OverallCreditLimit,
    OverdueDays,
    OverdueDaysDesc,
    OverduePaymentsAmount,
    OverduePaymentsNumber,
    PaymentMethod,
    PaymentMethodDesc,
    PaymentPeriodicity,
    PaymentPeriodicityDesc,
    PremiumCard,
    PremiumCardDesc,
    ReorganizedCreditCode,
    ReorganizedCreditCodeDesc,
    ServicesLinesNo,
    TimesCardUsed,
    TransactionType,
    TransactionTypeDesc,
    Utilization,
    LinkedSubject_CBSubjectCode,
    LinkedSubject_Name,
    LinkedSubject_Role,
    LinkedSubject_RoleDesc,
    Note_TypeDesc,
    Note_Text,
    Note_Type,
    run_date,
    NULL AS ContractRequestDate,
    'granted' AS SOURCE
  FROM
    prj-prod-dataplatform.risk_credit_cic_data.granted_contracts
  UNION ALL
    -- Query FOR dfnongranted
  SELECT
    digitalLoanAccountId,
    crifApplicationId,
    customerId,
    processEngineGuid,
    requestGuid,
    NULL AS ContractHistoryType,
    CBContractCode,
    NULL AS ContractEndDate,
    ContractPhase,
    ContractPhaseDesc,
    NULL AS ContractStartDate,
    NULL AS ContractStatus,
    NULL AS ContractStatusDesc,
    ContractType,
    ContractTypeDesc,
    NULL AS Currency,
    NULL AS CurrencyDesc,
    LastUpdateDate,
    NULL AS OriginalCurrency,
    NULL AS OriginalCurrencyDesc,
    ProviderCodeEncrypted,
    ProviderContractNo,
    ReferenceNo,
    Role,
    RoleDesc,
    NULL AS BilledAmount,
    NULL AS BoardResolutionFlag,
    NULL AS BoardResolutionFlagDesc,
    NULL AS CancellationDate,
    NULL AS CardReferenceCode,
    NULL AS ChargedAmount,
    CreditLimit,
    NULL AS CreditPurpose,
    NULL AS CreditPurposeDesc,
    FinancedAmount,
    NULL AS FirstPaymentDate,
    NULL AS FlagCardUsed,
    NULL AS HolderLiability,
    NULL AS HolderLiabilityDesc,
    NULL AS InstallmentType,
    NULL AS InstallmentTypeDesc,
    InstallmentsNumber,
    NULL AS LastChargeDate,
    NULL AS LastPaymentAmount,
    NULL AS LastPaymentDate,
    NULL AS MinPaymentIndicator,
    NULL AS MinPaymentIndicatorDesc,
    NULL AS MinPaymentPercentage,
    MonthlyPaymentAmount,
    NULL AS NextPayment,
    NULL AS NextPaymentDate,
    NULL AS OutstandingBalance,
    NULL AS OutstandingBalanceUnbilled,
    NULL AS OutstandingPaymentsNumber,
    NULL AS OverallCreditLimit,
    NULL AS OverdueDays,
    NULL AS OverdueDaysDesc,
    NULL AS OverduePaymentsAmount,
    NULL AS OverduePaymentsNumber,
    NULL AS PaymentMethod,
    NULL AS PaymentMethodDesc,
    PaymentPeriodicity,
    PaymentPeriodicityDesc,
    NULL AS PremiumCard,
    NULL AS PremiumCardDesc,
    NULL AS ReorganizedCreditCode,
    NULL AS ReorganizedCreditCodeDesc,
    NULL AS ServicesLinesNo,
    NULL AS TimesCardUsed,
    NULL AS TransactionType,
    NULL AS TransactionTypeDesc,
    NULL AS Utilization,
    LinkedSubject_CBSubjectCode,
    LinkedSubject_Name,
    LinkedSubject_Role,
    LinkedSubject_RoleDesc,
    Note_TypeDesc,
    Note_Text,
    Note_Type,
    run_date,
    ContractRequestDate,
    'nongranted' AS SOURCE
  FROM
    prj-prod-dataplatform.risk_credit_cic_data.notgranted_contracts )
,
employementdata as  --- data from cic employment table
(SELECT distinct
  digitalLoanAccountId, 
  crifApplicationId, 
  customerId,   
  AnnualMonthlyIndicator, 
  Currency, 
  DateHiredFrom, 
  DateHiredTo, 
  GrossIncome,
  CAST(
    CASE 
      WHEN COALESCE(AnnualMonthlyIndicator, 'NA') LIKE 'M' THEN CAST(COALESCE(GrossIncome, '0') AS NUMERIC)
      WHEN COALESCE(AnnualMonthlyIndicator, 'NA') LIKE 'Y' THEN ROUND(CAST(COALESCE(GrossIncome, '0') AS NUMERIC)/12, 0)
      ELSE 0 
    END AS INT64
  ) AS MonthlyIncome,  -- Calculated the monthly income
  CAST(
    CASE 
      WHEN COALESCE(AnnualMonthlyIndicator, 'NA') LIKE 'M' THEN ROUND(CAST(COALESCE(GrossIncome, '0') AS NUMERIC)*12, 0)
      WHEN COALESCE(AnnualMonthlyIndicator, 'NA') LIKE 'Y' THEN CAST(COALESCE(GrossIncome, '0') AS NUMERIC)
      ELSE 0 
    END AS INT64
  ) AS AnnualIncome,    ---Calculated the annual income
  OccupationDesc,
  OccupationStatusDesc,
  PSIC, 
  REGEXP_REPLACE(PSICDesc, r'^\d+\s*-\s*', '') AS PSICDesc ,
  row_number() over (partition by digitalLoanAccountId order by digitalLoanAccountId ) as rnk
FROM prj-prod-dataplatform.risk_credit_cic_data.employment_data),
CICBase2Table as  --- Combining all the above tables and creating some features
(SELECT digitalLoanAccountId, crifApplicationId, customerId,
       processEngineGuid, requestGuid, ContractHistoryType,
       CBContractCode, ContractEndDate, ContractPhase,
       ContractPhaseDesc, ContractStartDate, ContractStatus,
       ContractStatusDesc, ContractType, ContractTypeDesc,
       Currency, CurrencyDesc, LastUpdateDate, OriginalCurrency,
       OriginalCurrencyDesc, ProviderCodeEncrypted,
       ProviderContractNo, ReferenceNo, Role, RoleDesc,
       BilledAmount, BoardResolutionFlag, BoardResolutionFlagDesc,
       CancellationDate, CardReferenceCode, ChargedAmount,
       CreditLimit, CreditPurpose, CreditPurposeDesc,
       FinancedAmount, FirstPaymentDate, FlagCardUsed,
       HolderLiability, HolderLiabilityDesc, InstallmentType,
       InstallmentTypeDesc, InstallmentsNumber, LastChargeDate,
       LastPaymentAmount, LastPaymentDate, MinPaymentIndicator,
       MinPaymentIndicatorDesc, MinPaymentPercentage,
       MonthlyPaymentAmount, NextPayment, NextPaymentDate,
       OutstandingBalance, OutstandingBalanceUnbilled,
       OutstandingPaymentsNumber, OverallCreditLimit, OverdueDays,
       OverdueDaysDesc, OverduePaymentsAmount,
       OverduePaymentsNumber, PaymentMethod, PaymentMethodDesc,
       PaymentPeriodicity, PaymentPeriodicityDesc, PremiumCard,
       PremiumCardDesc, ReorganizedCreditCode,
       ReorganizedCreditCodeDesc, ServicesLinesNo, TimesCardUsed,
       TransactionType, TransactionTypeDesc, Utilization,
       LinkedSubject_CBSubjectCode, LinkedSubject_Name,
       LinkedSubject_Role, LinkedSubject_RoleDesc, Note_TypeDesc,
       Note_Text, Note_Type, run_date, ContractRequestDate,  SOURCE
,
  CASE
    WHEN ContractPhaseDesc = 'Active' AND ContractStatusDesc = '' THEN 'Neutral'
    WHEN ContractPhaseDesc = 'Active' AND ContractStatusDesc is null THEN 'Neutral'
    WHEN ContractPhaseDesc = 'Active' AND ContractStatusDesc = 'Pre-Activated' THEN 'Good'
    WHEN ContractPhaseDesc = 'Active' AND ContractStatusDesc = 'Foreclosure' THEN 'Good'
    WHEN ContractPhaseDesc = 'Closed' AND ContractStatusDesc = '' THEN 'Good'
    WHEN ContractPhaseDesc = 'Closed' AND ContractStatusDesc is null THEN 'Good'
    WHEN ContractPhaseDesc = 'Closed in advance' AND ContractStatusDesc = '' THEN 'Good'
    WHEN ContractPhaseDesc = 'Closed in advance' AND ContractStatusDesc is null THEN 'Good'
    WHEN ContractPhaseDesc = 'Closed in advance' AND ContractStatusDesc = 'Foreclosure' THEN 'Good'
    WHEN ContractStatusDesc IN ('Debt Assumption', 'Repossessed') THEN 'Neutral'
    WHEN ContractStatusDesc IN (
      'Write-off (BLW)', 'Past Due', 'Blocked by the Bank due to Credit Reasons',
      'Under dispute / non performing', 'Under litigation / Delinquent',
      'Blocked or Closed voluntary by the Customer', 'Blocked or Closed due to Restructuring',
      'There are unpaid amounts, Negotiated Settlement', 'Previous delinquency settled',
      'Write-off and Credit transferred to third party / Collection',
      'Write-off and Fully Settled', 'Blocked by the Bank due to card lost/stolen',
      'Blocked by the Bank due to fraud', 'Dispute / Litigation contested'
    ) THEN 'Bad'
    ELSE 'Unknown'
  END AS Repaymentcategory,
CASE
    WHEN ContractTypeDesc IN ('Salary loan', 'Personal Loan', 'Unsecured loan', 'Vehicle Loan', 'Mortgage/Real Estate', 'Time Loan', 'Short Term Loan', 'Benefit Loan', 'Home equity loan', 'Agricultural Loan', 'Student Loan', 'Vehicle leasing', 'Credit Card', 'Credit Card - Shared Limit', 'Credit Card - MultiCurrency', 'Revolving Credit', 'Trust Loan', 'Credit Line') 
      OR (ContractTypeDesc = 'Term Loan' AND CreditPurposeDesc NOT LIKE 'Small and Medium Enterprise Loans%')
      OR (ContractTypeDesc = 'Loan Line' AND CreditPurposeDesc NOT LIKE 'Small and Medium Enterprise Loans%')
      OR (CreditPurposeDesc LIKE 'Loans to Individual%' AND ContractTypeDesc != 'Business Loan')
      OR (CreditPurposeDesc LIKE 'Microfinance Loans' AND ContractTypeDesc != 'Business Loan')
      OR (CreditPurposeDesc LIKE 'Other Agricultural Credit' AND ContractTypeDesc != 'Business Loan')
      OR (ContractHistoryType LIKE 'Installments' AND ContractTypeDesc = 'Term Loan' and CreditPurposeDesc is null)
      OR (ContractHistoryType is null AND ContractTypeDesc = 'Term Loan' and CreditPurposeDesc is null)
      OR CreditPurposeDesc IN ('Agrarian Reform', 'Development Loan Incentives - Socialized Low Cost Housing (Loans to individuals for housing purposes )')
      OR ContractHistoryType = 'CreditCards'
    THEN 'B2C'
    
    WHEN ContractTypeDesc IN ('Business Loan', 'Real estate leasing', 'Equipment leasing')
      OR CreditPurposeDesc IN ('Development Loan Incentives - Cooperatives', 'Development Loan Incentives - Educational Inst.', 'Loan to Government - GOCCs (Other Financial)', 'Loan to Government - GOCCs (Social Security Institutions)', 'Loan to Government - LGUs', 'Loan to Government - National Government', 'Loans to Private Corporation (Financial)', 'Loans to Private Corporation (Non-Financial)', 'Small and Medium Enterprise Loans (Medium Scale Enterprise)', 'Small and Medium Enterprise Loans (Small Scale Enterprise)')
      OR (ContractTypeDesc = 'Vehicle Loan' AND CreditPurposeDesc NOT LIKE 'Loans to Individual%')
      OR (ContractTypeDesc = 'Loan Line' AND CreditPurposeDesc LIKE 'Small and Medium Enterprise Loans%')
      OR (ContractTypeDesc = 'Term Loan' AND CreditPurposeDesc LIKE 'Small and Medium Enterprise Loans%')
    THEN 'B2B'
    
    ELSE 'Unknown'
  END AS BusinessType,
 CASE
    WHEN ContractTypeDesc = 'Time Loan' THEN 'Time Loans'
    WHEN ContractTypeDesc IN ('Short Term Loan', 'Term Loan') THEN 'Short and Term Loans'
    WHEN ContractTypeDesc = 'Home equity loan' THEN 'Home Equity Loans'
    WHEN ContractTypeDesc IN ('Credit Card', 'Credit Card - MultiCurrency', 'Credit Card - Shared Limit') THEN 'Credit Cards'
    WHEN ContractTypeDesc IN ('Loan Line', 'Credit Line') THEN 'Credit Lines'
    WHEN ContractTypeDesc IN ('Mortgage/Real Estate', 'Real estate leasing') THEN 'Real Estate Loans'
    WHEN ContractTypeDesc = 'Trust Loan' THEN 'Trust Loans'
    WHEN ContractTypeDesc = 'Personal Loan' THEN 'Personal Loans'
    ELSE 'Other Loans'
  END AS loan_segment
from CICBaseTable 
  where COALESCE(ContractHistoryType, 'NA') in ('Installments', 'CreditCards', 'NA')
  and COALESCE(RoleDesc, 'NA') in ('Borrower', 'Co-Borrower', 'NA')
),
CICBase3Table as  ---- Filtering only the B2C and unknown. Excluding B2B
(select distinct * FROM  CICBase2Table where BusinessType in ('B2C', 'Unknown')
)
,
custtname ---- Getting the customer name from the loan customer details table
as 
(SELECT distinct  cast(custId as numeric) custid, firstName, middleName, LastName FROM `prj-prod-dataplatform.dl_loans_db_raw.tdbk_loan_customer_details` 
),
stepAtablebase as
(
select 
(a.digitalLoanAccountid||b.crifApplicationId||b.run_date||b.CBContractCode) uniquekey,
a.digitalLoanAccountId,
a.customerId, cn.Firstname, cn.middleName, cn.LastName,
a.loanAccountNumber,
a.flagDisbursement,
a.disbursementDateTime,
a.termsAndConditionsSubmitDateTime,
a.natureofwork,
a.subIndustryDescription,
a.industryDescription,
case when a.reloan_flag = 1 and a.loantype not like 'FLEXUP'then 'Reloan'
      when a.loantype = 'FLEXUP' and a.new_loan_type = 'Flex-up' and a.reloan_flag = 0 and a.flagDisbursement = 1 then 'Flex-up' 
              else a.new_loan_type end as LoanProduct, b.crifApplicationId, 
       processEngineGuid, requestGuid, ContractHistoryType,
       CBContractCode, ContractEndDate, ContractPhase,
       ContractPhaseDesc, ContractStartDate, ContractStatus,
       ContractStatusDesc, ContractType, ContractTypeDesc,
       b.Currency, CurrencyDesc, LastUpdateDate, OriginalCurrency,
       OriginalCurrencyDesc, ProviderCodeEncrypted,
       ProviderContractNo, ReferenceNo, Role, RoleDesc,
       BilledAmount, BoardResolutionFlag, BoardResolutionFlagDesc,
       CancellationDate, CardReferenceCode, ChargedAmount,
       CreditLimit, CreditPurpose, CreditPurposeDesc,
       FinancedAmount, FirstPaymentDate, FlagCardUsed,
       HolderLiability, HolderLiabilityDesc, InstallmentType,
       InstallmentTypeDesc, InstallmentsNumber, LastChargeDate,
       LastPaymentAmount, LastPaymentDate, MinPaymentIndicator,
       MinPaymentIndicatorDesc, MinPaymentPercentage,
       MonthlyPaymentAmount, NextPayment, NextPaymentDate,
       b.OutstandingBalance, OutstandingBalanceUnbilled,
       OutstandingPaymentsNumber, OverallCreditLimit, OverdueDays,
       OverdueDaysDesc, OverduePaymentsAmount,
       OverduePaymentsNumber, PaymentMethod, PaymentMethodDesc,
       PaymentPeriodicity, PaymentPeriodicityDesc, PremiumCard,
       PremiumCardDesc, ReorganizedCreditCode,
       ReorganizedCreditCodeDesc, ServicesLinesNo, TimesCardUsed,
       TransactionType, TransactionTypeDesc, Utilization,
       LinkedSubject_CBSubjectCode, LinkedSubject_Name,
       LinkedSubject_Role, LinkedSubject_RoleDesc, Note_TypeDesc,
       Note_Text, Note_Type, run_date, ContractRequestDate,  SOURCE, Repaymentcategory, BusinessType, loan_segment
       , ed.AnnualMonthlyIndicator, ed.Currency, ed.DateHiredFrom, ed.DateHiredTo, ed.GrossIncome, ed.MonthlyIncome, ed.AnnualIncome, ed.OccupationDesc, ed.OccupationStatusDesc, ed.PSIC, ed.PSICDesc
FROM `risk_credit_mis.loan_master_table` a 
inner join CICBase3Table b
ON a.digitalLoanAccountId = b.digitalLoanAccountId
    AND a.crifApplicationId = b.crifApplicationId
left join (select * from employementdata where rnk = 1) ed on ed.digitalLoanAccountId = a.digitalLoanAccountId
Left join custtname cn on cn.custid = a.customerId
where a.disbursementDateTime is not null
and date_trunc(a.disbursementDateTime, day) >= '2023-01-10'   ---- filtering the data based on the CIC pull date as agreed over the call
and date_trunc(a.disbursementDateTime, day) < current_date()
),
stepAtable2base as 
(select *, row_number() over(partition by uniquekey order by uniquekey) rnk from stepAtablebase)
,
base as 
(select a.*, d.obsFSPD30, d.defFSPD30 
, case when date_trunc(a.disbursementDateTime, day) <= '2024-03-31' then 'Train_Validation' 
         when date_trunc(a.disbursementDateTime, day) >'2024-03-31' and date_trunc(a.disbursementDateTime, day) <= '2024-04-30' then 'Test' else 'Other' end targetdataselectiontype
from stepAtable2base a 
inner join ---- joining the delinquency part to add the bad rates to the main query
(
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
where 
a.rnk = 1
and a.LoanProduct in ('Quick', 'SIL-Instore')
)
---- Finally calling the data.
select * from base 
;