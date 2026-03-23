# Student Daily Attendance - Business Rules

This document explains the business logic behind `fct_student_daily_attendance`, a fact table that produces **one record per student, school, and calendar date** with daily attendance status and cumulative metrics.

---

## Configurable Variables

These values can be customized in `dbt_project.yml`:

| Variable | Default | Purpose |
|----------|---------|---------|
| `edu:attendance:in_attendance_code` | `"In Attendance"` | Code assigned when no attendance event exists (student is present) |
| `edu:attendance:chronic_absence_threshold` | `90` | Attendance rate ≤ this percentage marks a student as chronically absent |
| `edu:attendance:chronic_absence_min_days` | `20` | Minimum days enrolled before chronic absenteeism is evaluated |
| `edu:attendance:daily_attendance_source` | `fct_student_school_attendance_event` | Source model for attendance events (can be overridden) |

---

## Building the Attendance Calendar

### Which Days Are Included?

The model only counts **instructional days** (`is_school_day = true`) and applies the following date filters:

**For Current School Year:**
- Date must be ≤ today (no future dates)
- Date must be ≤ the most recent submitted attendance event for that school

**For Past School Years:**
- All instructional days are included once the school calendar has ended
- This ensures the full school year is counted even if attendance submission stopped before the last day

### Student-Specific Calendar

Each student's attendance calendar is conditional on their enrollment:
- Starts at their `entry_date`
- Continues through the end of the calendar (even after withdrawal) to support rolling metrics

---

## Positive Attendance Fill

This is the core logic of the model. It creates a record for every instructional day a student should have attendance tracked.

### How Attendance Status Is Determined

| Scenario | `is_enrolled` | `attendance_event_category` | `is_absent` | `is_present` |
|----------|---------------|------------------------------|-------------|--------------|
| **During enrollment, no event recorded** | 1.0 | "In Attendance" | 0.0 | 1.0 |
| **During enrollment, absence event exists** | 1.0 | *(from source data)* | *(from source)* | 1.0 − is_absent |
| **After exit/withdrawal date** | 0.0 | "Not Enrolled" | 0.0 | 0.0 |

**Key Points:**
- If a student is enrolled and no attendance event exists for a day, they are assumed **present**
- `is_absent` and `is_present` can be **fractional** (e.g., 0.5 for half-day absence)
- Days after a student withdraws are tracked but marked as "Not Enrolled" with zero presence/absence

---

## Handling Overlapping Enrollments

Students can have multiple overlapping enrollments at the same school (e.g., re-enrollment, program changes). The model deduplicates to ensure each day is counted only once.

**Deduplication Priority:**
1. Prefer records where student **is enrolled** (1.0 beats 0.0)
2. Prefer **smaller sessions** (quarter over semester over full year)
3. Alphabetical by attendance event category
4. By session key as final tiebreaker

This handles scenarios where attendance events are linked to different session levels (e.g., Fall Semester vs. Year Round sessions with overlapping date ranges).

---

## Excusal Status Streaks

The model tracks consecutive days a student has the same excusal status using a "gaps and islands" pattern.

**Output:** `consecutive_days_by_excusal_status`

**Example:** If a student has 5 consecutive unexcused absences, this field would show 1, 2, 3, 4, 5 on those respective days.

---

## Cumulative Metrics

All cumulative metrics are calculated **per student, per school**, ordered by date:

| Metric | Description |
|--------|-------------|
| `total_days_enrolled` | Total days the student was enrolled at this school (across all time) |
| `cumulative_days_absent` | Running total of absences up to this date |
| `cumulative_days_attended` | Running total of days present up to this date |
| `cumulative_days_enrolled` | Running total of enrolled days up to this date |
| `cumulative_attendance_rate` | `100 × cumulative_days_attended / cumulative_days_enrolled` (rounded to 2 decimals) |
| `meets_enrollment_threshold` | TRUE if student has been enrolled ≥ 20 days (configurable) |

---

## Chronic Absenteeism

A student is flagged as **chronically absent** (`is_chronic_absentee = TRUE`) when **both** conditions are met:

1. `cumulative_attendance_rate` ≤ **90%**
2. `cumulative_days_enrolled` ≥ **20 days**

The enrollment minimum prevents flagging students who have only been enrolled a few days.

> **Note:** Both thresholds are configurable via `dbt_project.yml`.

---

## Absentee Category Labels

Students are categorized based on their cumulative attendance rate using thresholds defined in the `absentee_categories` seed table.

**Expected seed structure:**

| threshold_lower | threshold_upper | level_numeric | level_label |
|-----------------|-----------------|---------------|-------------|
| 0 | 80 | 1 | Severe Chronic Absence |
| 80 | 90 | 2 | Chronic Absence |
| 90 | 95 | 3 | At Risk |
| 95 | 100 | 4 | Satisfactory |

**Output columns:**
- `absentee_category_rank` — Numeric rank (1 = most severe)
- `absentee_category_label` — Human-readable label

**Note:** Categories are only assigned if `meets_enrollment_threshold` is TRUE. Students with fewer than the minimum enrolled days will have NULL values for these fields.

---

## Data Flow Summary

```
╔═══════════════════════════════════════════════════════════════════════════════╗
║                              DATA SOURCES                                     ║
╚═══════════════════════════════════════════════════════════════════════════════╝

┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ dim_calendar_   │  │  dim_session    │  │ fct_student_    │  │ fct_student_    │
│     date        │  │                 │  │ school_assoc    │  │ school_attend_  │
│                 │  │ • session dates │  │                 │  │     event       │
│ • school days   │  │ • total instr.  │  │ • entry/exit    │  │                 │
│ • calendar      │  │   days          │  │   dates         │  │ • is_absent     │
│   dates         │  │                 │  │ • school        │  │ • excusal       │
│                 │  │                 │  │   calendar      │  │   status        │
└────────┬────────┘  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘
         │                    │                    │                    │
         │                    ▼                    │                    │
         │           ┌─────────────────┐           │                    │
         │           │ bld_ef3__       │           │                    │
         │           │ attendance_     │           │                    │
         │           │ sessions        │           │                    │
         │           │                 │           │                    │
         │           │ • maps schools  │           │                    │
         │           │   to sessions   │           │                    │
         │           └────────┬────────┘           │                    │
         │                    │                    │                    │
         ▼                    ▼                    ▼                    ▼
╔═══════════════════════════════════════════════════════════════════════════════╗
║                           TRANSFORMATION STEPS                                ║
╚═══════════════════════════════════════════════════════════════════════════════╝

┌───────────────────────────────────────────────────────────────────────────────┐
│  1. Build Attendance Calendar                                                 │
│     • Instructional days only (is_school_day = true)                          │
│     • Filter: date ≤ today AND date ≤ max submitted attendance by school      │
│     • Past school years: include all days once calendar has ended             │
│                                                                               │
│     Sources: dim_calendar_date                                                │
└───────────────────────────────────────────────────────────────────────────────┘
                                       ↓
┌───────────────────────────────────────────────────────────────────────────────┐
│  2. Create Student-Enrollment Calendar                                        │
│     • Join students to attendance calendar via school calendar                │
│     • Include days from entry_date through end of calendar                    │
│                                                                               │
│     Sources: + fct_student_school_association                                 │
└───────────────────────────────────────────────────────────────────────────────┘
                                       ↓
┌───────────────────────────────────────────────────────────────────────────────┐
│  3. Fill Positive Attendance                                                  │
│     • LEFT JOIN attendance events to student calendar                         │
│     • No event = "In Attendance" (student assumed present)                    │
│     • Calculate: is_enrolled, is_absent, is_present                           │
│     • Post-withdrawal days marked "Not Enrolled"                              │
│                                                                               │
│     Sources: + fct_student_school_attendance_event                            │
│              + dim_session, bld_ef3__attendance_sessions                      │
└───────────────────────────────────────────────────────────────────────────────┘
                                       ↓
┌───────────────────────────────────────────────────────────────────────────────┐
│  4. Deduplicate Overlapping Enrollments                                       │
│     • One record per student/school/date                                      │
│     • Priority: enrolled > not enrolled, smaller sessions preferred           │
└───────────────────────────────────────────────────────────────────────────────┘
                                       ↓
┌───────────────────────────────────────────────────────────────────────────────┐
│  5. Calculate Excusal Status Streaks                                          │
│     • Track consecutive days with same excusal status                         │
│     • Uses gaps-and-islands pattern                                           │
└───────────────────────────────────────────────────────────────────────────────┘
                                       ↓
┌───────────────────────────────────────────────────────────────────────────────┐
│  6. Compute Cumulative Metrics                                                │
│     • cumulative_days_absent, cumulative_days_attended                        │
│     • cumulative_attendance_rate = 100 × attended / enrolled                  │
│     • is_chronic_absentee = rate ≤ 90% AND enrolled ≥ 20 days                 │
└───────────────────────────────────────────────────────────────────────────────┘
                                       ↓
┌───────────────────────────────────────────────────────────────────────────────┐
│  7. Apply Absentee Category Labels                                            │
│     • Join attendance rate to threshold bands                                 │
│     • Assign absentee_category_rank and absentee_category_label               │
│     • Only applied if meets_enrollment_threshold = TRUE                       │
│                                                                     ┌────────────────┐
│     Sources: + absentee_categories (seed)                           │ absentee_      │
│                                                                     │ categories     │
│                                                                     │                │
│                                                                     │ • rate bands   │
│                                                                     │ • labels       │
│                                                                     └────────────────┘
└───────────────────────────────────────────────────────────────────────────────┘
                                       ↓
╔═══════════════════════════════════════════════════════════════════════════════╗
║                              FINAL OUTPUT                                     ║
║                                                                               ║
║                    fct_student_daily_attendance                               ║
║                    One row per student / school / date                        ║
╚═══════════════════════════════════════════════════════════════════════════════╝
```

---

## Key Output Columns

| Column | Description |
|--------|-------------|
| `k_student`, `k_school`, `calendar_date` | Identifies the record |
| `attendance_event_category` | Descriptor value (e.g., "In Attendance", "Excused Absence") |
| `is_absent` | Absence indicator (can be fractional for partial days) |
| `is_present` | Presence indicator (inverse of is_absent) |
| `is_enrolled` | 1.0 during enrollment, 0.0 after withdrawal |
| `attendance_excusal_status` | Excused, Unexcused, In Attendance, or Not Enrolled |
| `consecutive_days_by_excusal_status` | Count of consecutive days with same excusal status |
| `cumulative_attendance_rate` | Percentage, updated daily |
| `is_chronic_absentee` | TRUE if rate ≤ 90% and enrolled ≥ 20 days |
| `absentee_category_rank` | Numeric severity level |
| `absentee_category_label` | Text label for attendance category |

