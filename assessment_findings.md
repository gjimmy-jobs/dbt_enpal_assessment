## Enpal Assessment Approach and Oberservations

All data validation queries and integrity checks are documented in `analyses/analysis_queries.sql`.

### Schema Overview

**Dimension Tables** (low complexity):
- `activity_types` – Activity classifications
- `fields` – Field definitions
- `stages` – Deal stage definitions
- `users` – User directory

These tables require minimal validation. However, `users` has data quality concerns: duplicate emails and names exist but uniqueness constraints were not enforced in the dbt schema. Consider building a separate model to detect and flag these duplicates if this becomes a critical concern.

**Fact Tables** (higher complexity):
- `activity` – Activity records linked to deals
- `deal_changes` – Deal event history and field changes

### Key Data Quality Findings

**Critical Issue: Deal ID Overlap**
- Activities reference 4,572 unique deal_ids
- Deal changes reference 1,995 unique deal_ids
- **Overlap: only 8 deals in common** (4,564 activity-only, 1,987 stage-only)
- This massive mismatch suggests either: (1) activities logged on deals that never progressed, (2) deals progressed but activities weren't tracked, or (3) data extraction inconsistency
- Recommendation: Investigate root cause before building the funnel model

**Data Integrity Issues**
- 5 duplicate deal_ids detected in `deal_changes` with multiple `add_time` records and conflicting stage progressions
- These duplicates are retained in the staging layer pending investigation
- All other deals follow expected patterns:
  - Sequential stage progression (no backwards moves - some steps are skipped forwards)
  - Users assigned before stage changes
  - All deals start at stage 1
  - All deals end with `lost_reason` marker

### Data Modeling Approach

**Layer Strategy** (stg → int → mart):
- **Staging (stg_)**: Standardize raw table schemas, apply type casting, rename columns for consistency
- **Intermediate (int_)**: Denormalize deal + stage timeline once for reusability across reports
- **Marts (rep_)**: Expose clean, grain-specific reporting tables (e.g., `rep_sales_funnel_monthly`)

**Naming & Quality Standards**:
- Pluralize table names (e.g., `activities` instead of `activity`)
- Standardize column naming conventions per dbt best practices
- Convert text-based flags ('TRUE'/'FALSE', 'yes'/'no') to boolean type
- Apply dbt tests to all staging tables:
  - `not_null` – on key identifiers
  - `unique` – on primary keys
  - `accepted_values` – on enums (stage names, activity types)
  - `relationships` – on foreign keys

**Data Normalization**:
- Split `deal_changes` into field-specific tables where appropriate (e.g., `deal_stages`, `deal_users`) for clarity
- This separates semantically different change types (stage progression ≠ user assignment timeline)

**Code Quality**:
- Format all SQL with sqlfluff for consistency
- Document assumptions and data quality issues in model headers
- Add comments for larger queries or models