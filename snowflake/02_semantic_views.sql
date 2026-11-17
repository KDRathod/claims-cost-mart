-- =============================================================================
-- Semantic views: the governed layer Cortex Analyst is allowed to use.
--
-- Why a semantic layer instead of pointing an LLM at the tables:
--   * Metric definitions live here once (denial rate, PMPM, IBNR), so the
--     model picks a definition instead of inventing one.
--   * Only de-identified marts are exposed. RESTRICTED.DIM_MEMBER_PII is not
--     part of any semantic view, so the model cannot select a name or SSN.
--   * Joins are declared, not guessed.
--
-- Deployed by ai/deploy_semantic_views.py as CLAIMS_TRANSFORMER.
-- =============================================================================

create or replace semantic view CLAIMS.MARTS.CLAIMS_ANALYTICS
  tables (
    claims as CLAIMS.MARTS.FCT_CLAIMS
      primary key (claim_id)
      with synonyms ('claim', 'medical claims')
      comment = 'One row per medical claim with its current financial state. Service dates run from 2023-01-01 to 2026-09-30.',
    payers as CLAIMS.MARTS.DIM_PAYER
      primary key (payer_id)
      with synonyms ('plan', 'insurer', 'health plan')
      comment = 'Payers (health plans). payer_type is government or private.',
    members as CLAIMS.MARTS.DIM_MEMBER
      primary key (member_id)
      with synonyms ('patient', 'enrollee')
      comment = 'De-identified members. No names, SSNs or street addresses exist in this model.'
  )
  relationships (
    claims_to_payers as claims (payer_id) references payers,
    claims_to_members as claims (member_id) references members
  )
  facts (
    claims.billed_amount as billed_amount comment = 'Amount charged by the provider',
    claims.allowed_amount as allowed_amount comment = 'Contractually allowed amount after adjustments',
    claims.paid_amount as paid_amount comment = 'Net amount the plan has paid to date, after adjustments and reversals',
    claims.member_responsibility_amount as member_responsibility_amount comment = 'Cost share owed by the member',
    claims.days_submission_to_decision as days_submission_to_decision comment = 'Days from claim submission to the first paid or denied decision'
  )
  dimensions (
    claims.claim_id as claim_id,
    claims.service_date as service_date comment = 'Date the care was delivered. Use this for calendar filters such as a year or quarter.',
    claims.service_month as service_month comment = 'First day of the month of service',
    claims.service_category as service_category
      with synonyms ('category of care')
      comment = 'One of facility_stay (inpatient, skilled nursing, hospice), emergency_urgent, outpatient, primary_preventive',
    claims.claim_type as claim_type comment = 'professional or institutional',
    claims.claim_status as claim_status
      comment = 'Current status: pending (no decision yet), paid, denied, reversed',
    claims.denial_reason_code as denial_reason_code comment = 'Reason code of the denial, for claims that were ever denied',
    claims.provider_network_status as provider_network_status_at_service
      with synonyms ('network status')
      comment = 'in_network or out_of_network on the service date',
    claims.was_ever_denied as was_ever_denied comment = 'True if the claim was denied at any point, even if later paid',
    claims.was_adjusted as was_adjusted comment = 'True if the payment was adjusted after it was first paid',
    claims.was_reversed as was_reversed comment = 'True if a payment on the claim was reversed',
    claims.was_overturned_on_appeal as was_overturned_on_appeal comment = 'True if a denial was overturned and the claim was then paid',
    payers.payer_name as payer_name with synonyms ('plan name'),
    payers.payer_type as payer_type comment = 'government or private',
    members.age_band as age_band comment = 'One of 0-17, 18-34, 35-49, 50-64, 65+',
    members.gender as gender,
    members.county as county
  )
  metrics (
    claims.claim_count as count(claims.claim_id) comment = 'Number of claims',
    claims.member_count as count(distinct claims.member_id) comment = 'Number of distinct members with a claim',
    claims.total_billed_amount as sum(claims.billed_amount),
    claims.total_allowed_amount as sum(claims.allowed_amount),
    claims.total_paid_amount as sum(claims.paid_amount)
      with synonyms ('paid', 'cost', 'spend', 'plan paid')
      comment = 'Total net paid to date',
    claims.denied_claim_count as count_if(claims.claim_status = 'denied') comment = 'Claims whose current status is denied',
    claims.denial_rate_pct as 100 * count_if(claims.claim_status = 'denied') / nullif(count(claims.claim_id), 0)
      comment = 'Percent of claims whose current status is denied (0 to 100)',
    claims.adjusted_claim_count as count_if(claims.was_adjusted),
    claims.reversed_claim_count as count_if(claims.was_reversed),
    claims.average_paid_per_paid_claim as avg(case when claims.claim_status = 'paid' then claims.paid_amount end)
      comment = 'Average net paid amount across claims with status paid',
    claims.average_days_to_decision as avg(claims.days_submission_to_decision)
  )
  comment = 'Claim-level analytics: counts, paid amounts, denials and turnaround, by payer, member group and service category.'
  ai_sql_generation 'Filter calendar periods on service_date. Report rates as percentages from 0 to 100. Round money to 2 decimals. This model has no member names, SSNs, addresses or dates of birth: if a question asks for them, say the data is not available and do not write SQL. Do not forecast future periods.';

-- Metric names must differ from the physical column names their facts read,
-- otherwise Snowflake reports a cyclic reference.
create or replace semantic view CLAIMS.MARTS.COST_PMPM
  tables (
    pmpm as CLAIMS.MARTS.MART_PMPM_MONTHLY
      primary key (pmpm_month_id)
      with synonyms ('per member per month', 'cost trend')
      comment = 'One row per service month and payer: member months, paid to date and the completion-adjusted estimate of final cost.'
  )
  facts (
    pmpm.member_months_fact as member_months,
    pmpm.paid_to_date_fact as paid_to_date,
    pmpm.estimated_ultimate_paid_fact as estimated_ultimate_paid,
    pmpm.ibnr_estimate_fact as ibnr_estimate
  )
  dimensions (
    pmpm.service_month as service_month comment = 'First day of the month of service. Filter months and years on this.',
    pmpm.payer_name as payer_name,
    pmpm.payer_type as payer_type comment = 'government or private',
    pmpm.is_mature as is_mature comment = 'True when the month has at least 12 months of run-out, so its cost is final',
    pmpm.months_of_runout as months_of_runout comment = 'Months elapsed since the service month'
  )
  metrics (
    pmpm.total_member_months as sum(pmpm.member_months_fact) comment = 'Members enrolled, counted once per month',
    pmpm.total_paid_to_date as sum(pmpm.paid_to_date_fact) comment = 'Amount actually paid so far',
    pmpm.total_estimated_ultimate_paid as sum(pmpm.estimated_ultimate_paid_fact)
      comment = 'Completion-adjusted estimate of what the month will finally cost',
    pmpm.total_ibnr_estimate as sum(pmpm.ibnr_estimate_fact)
      with synonyms ('IBNR', 'incurred but not reported')
      comment = 'Estimated cost of care already delivered but not yet paid',
    pmpm.pmpm_paid_to_date as sum(pmpm.paid_to_date_fact) / nullif(sum(pmpm.member_months_fact), 0)
      with synonyms ('PMPM', 'per member per month cost')
      comment = 'Paid to date divided by member months. Never average the row-level PMPM: always divide total paid by total member months.',
    pmpm.pmpm_estimated_ultimate as sum(pmpm.estimated_ultimate_paid_fact) / nullif(sum(pmpm.member_months_fact), 0)
      comment = 'Completion-adjusted PMPM'
  )
  comment = 'Per member per month cost, IBNR and member months by service month and payer.'
  ai_sql_generation 'PMPM is always total paid divided by total member months, never an average of PMPM values. Round money to 2 decimals. Do not forecast future periods.';
