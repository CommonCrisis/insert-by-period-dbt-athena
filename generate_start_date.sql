
{% macro generate_start_date(days_before_today) -%}
  {{- return(adapter.dispatch('generate_start_date', 'insert_by_period_v2')(days_before_today)) -}}
{%- endmacro %}

{% macro default__generate_start_date(days_before_today) -%}
  {{- dbt_utils.log_info('Calculating start_date from "today - ' ~ days_before_today ~ ' days"') -}}
  {%- set delta_days = days_before_today | int -%}
  {%- if delta_days != 0 -%}
    {%- set sql_statement -%}
      select date_add('day', -{{ delta_days }}, current_date)
    {%- endset -%}
    {%- set start_date = dbt_utils.get_single_value(sql_statement, default="2024-01-01") -%}
    {{- return(start_date) -}}
  {%- else -%}
    {%- set error_message -%}
      Value for days_before_today '{{ days_before_today }}' needs to be a stringified integer
    {%- endset -%}
    {{- exceptions.raise_compiler_error(error_message) -}}
  {%- endif -%}
{%- endmacro %}
