{#
  Cross-adapter compatibility macros
  ===================================
  These functions exist in DuckDB with different names or syntax than Spark SQL.
  Each macro dispatches to the correct dialect based on target.type.

  Usage:
    {{ date_diff_days(start, end) }}
    {{ date_diff_hours(start, end) }}
    {{ arg_max(column, ordering_column) }}
    {{ bool_or_agg(condition) }}

  Tested targets: duckdb (dev), databricks (production)
#}


{# ------------------------------------------------------------------
   date_diff_days
   DuckDB:  date_diff('day', start, end)
   Spark:   datediff(end, start)           ← note: reversed argument order
   ------------------------------------------------------------------ #}
{% macro date_diff_days(start_date, end_date) %}
    {%- if target.type == 'databricks' -%}
        datediff({{ end_date }}, {{ start_date }})
    {%- else -%}
        date_diff('day', {{ start_date }}, {{ end_date }})
    {%- endif -%}
{% endmacro %}


{# ------------------------------------------------------------------
   date_diff_hours
   DuckDB:  date_diff('hour', start, end)
   Spark:   (unix_timestamp(end) - unix_timestamp(start)) / 3600.0
   ------------------------------------------------------------------ #}
{% macro date_diff_hours(start_ts, end_ts) %}
    {%- if target.type == 'databricks' -%}
        (unix_timestamp({{ end_ts }}) - unix_timestamp({{ start_ts }})) / 3600.0
    {%- else -%}
        date_diff('hour', {{ start_ts }}, {{ end_ts }})
    {%- endif -%}
{% endmacro %}


{# ------------------------------------------------------------------
   arg_max  — pick the value of `column` from the row where `ordering` is largest
   DuckDB:  arg_max(column, ordering)
   Spark:   max_by(column, ordering)          ← Spark 3.0+, DBR 8.0+
   ------------------------------------------------------------------ #}
{% macro arg_max(column, ordering) %}
    {%- if target.type == 'databricks' -%}
        max_by({{ column }}, {{ ordering }})
    {%- else -%}
        arg_max({{ column }}, {{ ordering }})
    {%- endif -%}
{% endmacro %}


{# ------------------------------------------------------------------
   bool_or_agg  — true if ANY row in the group satisfies the condition
   DuckDB:  bool_or(condition)
   Spark:   CAST(MAX(CASE WHEN condition THEN 1 ELSE 0 END) AS BOOLEAN)
   ------------------------------------------------------------------ #}
{% macro bool_or_agg(condition) %}
    {%- if target.type == 'databricks' -%}
        cast(max(case when {{ condition }} then 1 else 0 end) as boolean)
    {%- else -%}
        bool_or({{ condition }})
    {%- endif -%}
{% endmacro %}
