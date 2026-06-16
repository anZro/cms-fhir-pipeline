# Claims Analysis

CMS BCDA Sandbox — Explanation of Benefits across 10,000 synthetic Medicare enrollees

```sql claims_summary
select * from bcda_fhir_bq.claims_summary
```

<BigValue
  data={claims_summary}
  value="total_claims"
  title="Total Claims"
  fmt="num0"
/>

<BigValue
  data={claims_summary}
  value="total_payments"
  title="Total Payments"
  fmt="usd0"
/>

<BigValue
  data={claims_summary}
  value="avg_payment_per_claim"
  title="Average Payment per Claim"
  fmt="usd2"
/>

## Claims by Type

```sql claims_by_type
select * from bcda_fhir_bq.claims_by_type
```

<BarChart
  data={claims_by_type}
  x="claim_type"
  y="claim_count"
  title="Claim Volume by Type"
/>

<DataTable data={claims_by_type}>
  <Column id="claim_type" title="Claim Type" />
  <Column id="claim_count" title="Claims" fmt="num0" />
  <Column id="total_payment" title="Total Payment" fmt="usd0" />
  <Column id="avg_payment" title="Avg Payment" fmt="usd2" />
</DataTable>

## Top 20 Diagnosis Codes

```sql top_diagnoses
select * from bcda_fhir_bq.top_diagnoses
```

<DataTable data={top_diagnoses}>
  <Column id="diagnosis_code" title="ICD-10 Code" />
  <Column id="diagnosis_display" title="Description" />
  <Column id="diagnosis_count" title="Claim Volume" fmt="num0" />
</DataTable>

## Monthly Claim Volume

```sql monthly_claim_volume
select * from bcda_fhir_bq.monthly_claim_volume
```

<LineChart
  data={monthly_claim_volume}
  x="claim_month"
  y="claim_count"
  title="Monthly Claim Volume (2015+)"
/>

> **March 2020 spike:** Claim volume across all categories surges in March 2020 — Outpatient claims more than quadruple (2,057 → 8,636), Inpatient claims increase 7x. This pattern is consistent with COVID-19's onset in the US and suggests BCDA's synthetic data incorporates realistic population-level utilization shifts rather than purely random generation. December 2020 reflects the end of the sandbox's data generation window, not a real decline.

> Scoped to 2015 onward for chart readability — the BCDA sandbox contains synthetic historical claims dating back to the 1980s. The full claims history remains queryable in `fct_claims`.