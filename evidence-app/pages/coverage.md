# Coverage & Eligibility

CMS BCDA Sandbox — Medicare coverage across 10,000 synthetic enrollees

## Medicare Part Distribution

```sql medicare_part_distribution
select * from bcda_fhir_bq.medicare_part_distribution
```

<BarChart
  data={medicare_part_distribution}
  x="medicare_part"
  y="enrollment_count"
  title="Coverage Records by Medicare Part"
/>

## Dual Eligibility

```sql dual_eligibility
select * from bcda_fhir_bq.dual_eligibility
```

<BarChart
  data={dual_eligibility}
  x="dual_eligible_status"
  y="patient_count"
  title="Dual Eligibility Status"
/>

> **Note on dual eligibility data:** The `dual_eligible` field is not
> populated in the BCDA sandbox Coverage resources (see Data Quality
> Findings in the README). This chart will show all patients as "Unknown"
> until production data with this field populated is available.

## Service Activity by Birth Cohort

```sql enrollment_activity
select * from bcda_fhir_bq.enrollment_activity
```

<LineChart
  data={enrollment_activity}
  x="enrollment_year"
  y="patient_count"
  title="Patients by Earliest Service Date"
/>

> **Note on this metric:** True Coverage period dates aren't populated in
> the BCDA sandbox, so this uses each patient's earliest EOB service date
> as a proxy. The resulting bell-shaped distribution — peaking around
> 1965-1972 — reflects birth-cohort timing relative to Medicare eligibility
> rather than actual enrollment events. This is a known limitation of using
> service dates as an enrollment proxy, surfaced by inspecting the chart
> output rather than assumed from the schema.