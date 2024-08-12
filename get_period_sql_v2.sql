{% macro get_period_sql_v2(target_cols_csv, sql, timestamp_field, start_timestamp, stop_timestamp, period, is_timestamp_string, timestamp_format, offset, additional_filter_logic) -%}
    {{ return(adapter.dispatch('get_period_sql_v2', 'insert_by_period')(target_cols_csv, sql, timestamp_field, start_timestamp, stop_timestamp, period, is_timestamp_string, timestamp_format, offset, additional_filter_logic)) }}
{% endmacro %}

{% macro default__get_period_sql_v2(target_cols_csv, sql, timestamp_field, start_timestamp, stop_timestamp, period, is_timestamp_string, timestamp_format, offset, additional_filter_logic) -%}
    {%- set period_length = period.split(' ')[0]| trim | int -%}
    {%- set period_type = period.split(' ')[1]| trim | string -%}
    {%- set start_period_interval = period_length * offset -%}
    {%- set stop_period_interval = period_length * (offset +1) -%}

    {%- set period_filter -%}
        {% if is_timestamp_string == True %}
            (date_parse({{ timestamp_field }}, '{{ timestamp_format }}') >=  timestamp '{{ start_timestamp }}'
            + interval '{{ start_period_interval }}' {{ period_type }} and
            date_parse({{ timestamp_field }}, '{{ timestamp_format }}') < timestamp '{{ start_timestamp }}'
            + interval '{{ stop_period_interval }}' {{ period_type }}  and
            date_parse({{ timestamp_field }}, '{{ timestamp_format }}') <  timestamp '{{ stop_timestamp }}')
            {{ ' AND ' ~ additional_filter_logic if additional_filter_logic else '' }}
        {% else %}
            ({{ timestamp_field }} >= timestamp '{{ start_timestamp }}'
            + interval '{{ start_period_interval }}' {{ period_type }} and
            {{ timestamp_field }} < timestamp '{{ start_timestamp }}'
            + interval '{{ stop_period_interval }}' {{ period_type }}  and
            {{ timestamp_field }} <  timestamp '{{ stop_timestamp }}')
            {{ ' AND ' ~ additional_filter_logic if additional_filter_logic else '' }}
        {% endif %}
    {%- endset -%}


    {%- set filtered_sql = sql | replace("__period_filter__", period_filter) -%}

    select
        {{ target_cols_csv }}
    from (
        {{ filtered_sql }}
    )

{%- endmacro %}
