# Population Overview

CMS BCDA Sandbox — 10,000 synthetic Medicare enrollees

```sql population_overview
select * from bcda_fhir_bq.population_overview
```

<BigValue
  data={population_overview}
  value="total_beneficiaries"
  title="Total Beneficiaries"
  fmt="num0"
/>

## Gender Distribution

```sql gender_distribution
select * from bcda_fhir_bq.gender_distribution
```

<BarChart
  data={gender_distribution}
  x="gender"
  y="patient_count"
  title="Beneficiaries by Gender"
  fmt="num0"
/>

## Race & Ethnicity Distribution

```sql race_distribution
select * from bcda_fhir_bq.race_distribution
```

<BarChart
  data={race_distribution}
  x="race_display"
  y="patient_count"
  title="Beneficiaries by Race/Ethnicity"
  swapXY={true}
  fmt="num0"
/>

> **Note on geographic data:** All 10,000 synthetic enrollees in the BCDA sandbox share the same SSA state code (`22`) in the Patient resource. This appears to be a characteristic of CMS's synthetic data generation rather than a pipeline defect — confirmed by inspecting raw FHIR Patient resources directly. State-level analysis is supported in the schema for production use with real multi-state populations.