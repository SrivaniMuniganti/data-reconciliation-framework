# Schema Mapping File — Column Reference

Place Excel mapping files (`.xlsx`) in this directory.

Each file describes how one dataset's origin columns map to its destination
columns and which transformation rules to apply.

---

## Required Columns

| Column | Description | Example |
|---|---|---|
| `Source_Column` | Column name in the origin dataset | `employee_id` |
| `Target_Column` | Column name in the destination dataset | `emp_id` |
| `Rule_Type` | Transformation rule to apply (see below) | `direct` |
| `Is_Key` | Join key indicator (`Y` or `N`) | `Y` |

## Optional Columns

| Column | Description | Default |
|---|---|---|
| `Apply_On` | Which side the rule applies to: `SOURCE`, `TARGET`, or `BOTH` | `BOTH` |
| `Parameters` | JSON object of rule-specific configuration | `{}` |
| `Cardinality` | Row cardinality hint: `1:1`, `1:N`, `N:1` | `1:1` |

---

## Available Transformation Rules

### `direct`
No transformation; the value is passed through as-is.

### `bool_to_int`
Converts boolean-like values to integers.
- `TRUE` / `true` / `1` → `1`
- `FALSE` / `false` / `0` → `0`

### `idmap`
Translates IDs using a separate lookup Excel file.

**Parameters (JSON):**
```json
{
  "lookup_column": "source_id",
  "return_column": "destination_id",
  "dedupe": "first"
}
```

| Parameter | Required | Description |
|---|---|---|
| `lookup_column` | ✅ | Column in the lookup file to match against |
| `return_column` | ✅ | Column in the lookup file to return |
| `dedupe` | ❌ | De-dup strategy: `first`, `last`, `most_common` |

### `strip_prefix`
Removes environment/batch prefixes from destination names and converts to uppercase.

**Example:**
- `"PHASE_1_Acme Corporation"` → `"ACME CORPORATION"`
- `"Batch3_Testing_Globex Ltd."` → `"GLOBEX LTD."`

---

## Example Mapping Row

| Source_Column | Target_Column | Rule_Type | Is_Key | Apply_On | Parameters |
|---|---|---|---|---|---|
| `src_employee_id` | `employee_id` | `idmap` | Y | BOTH | `{"lookup_column":"src_id","return_column":"dest_id"}` |
| `is_active` | `active_flag` | `bool_to_int` | N | BOTH | |
| `org_name` | `organisation_name` | `strip_prefix` | N | TARGET | |
| `email` | `email_address` | `direct` | N | BOTH | |
