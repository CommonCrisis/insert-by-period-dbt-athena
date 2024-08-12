{% macro get_period_boundaries_v2(target_schema,
    target_table,
    timestamp_field,
    start_date,
    stop_date,
    force_dates,
    period,
    is_timestamp_string,
    timestamp_format,
    additional_filter_logic,
    filter_columns
) -%}
    {{ return(
        adapter.dispatch('get_period_boundaries_v2', 'insert_by_period')(
            target_schema,
            target_table,
            timestamp_field,
            start_date,
            stop_date,
            force_dates,
            period,
            is_timestamp_string,
            timestamp_format,
            additional_filter_logic,
            filter_columns
        )
    )
    }}
{% endmacro %}

{% macro default__get_period_boundaries_v2(
    target_schema,
    target_table,
    timestamp_field,
    start_date,
    stop_date,
    force_dates,
    period,
    is_timestamp_string,
    timestamp_format,
    additional_filter_logic,
    filter_columns
)-%}
    {%- if target.name in ["merge", "local"] -%}
        -- Here you may remove the "local" or "merge" to allow a bigger built - then force_dates need to be set
        -- Or you set a start_date --> That sets force_date to true
        {% do dbt_utils.log_info("Running in test mode - this will always return 0 results") %}
        {% set period_boundaries = {
            'start_timestamp': "2024-01-02",
            'stop_timestamp': "2024-01-01",
            'num_periods': 0
        } %}
        with data as (
            select
            date_add('day', -1, current_date) AS start_timestamp,
            current_date AS stop_timestamp
        )
    {%- else -%}
        {%- set period_length = period.split(" ")[0] | trim | int -%}
        {%- set period_type = period.split(" ")[1] | trim | string -%}
        {%- set boundary_sql -%}
            with data as (
                select
                {% if force_dates %}
                    date_parse('{{ start_date }}', '{{ timestamp_format }}') AS start_timestamp,
                {% else %}
                    {% if is_timestamp_string == True %}
                        date_parse(coalesce(max({{ timestamp_field }})), '{{ timestamp_format }}') AS start_timestamp,
                    {% else %}
                        coalesce(max({{ timestamp_field }}), date_parse('{{ start_date }}', '{{ timestamp_format }}' )) AS start_timestamp,
                    {% endif %}
                {% endif %}
                    coalesce(date_parse(nullif('{{ stop_date }}', ''), '{{ timestamp_format }}'), current_date) AS stop_timestamp
                {% if not force_dates %}
                    {% if filter_columns %}
                        , {{ filter_columns | join(' ,') }}
                        from "{{ target_schema }}"."{{ target_table }}"
                        group by {{ filter_columns | join(' ,') }}
                    {% else %}
                        from "{{ target_schema }}"."{{ target_table }}"
                    {% endif %}
                {% endif %}
            )
            -- Ensure proper string conversion by using format_datetime. Athena cannot handle 6 microseconds, only 3!

            {% if additional_filter_logic and not force_dates %}
                select
                    format_datetime(start_timestamp, 'YYYY-MM-dd HH:mm:ss.SSS'),
                    format_datetime(stop_timestamp, 'YYYY-MM-dd HH:mm:ss.SSS'),
                    date_diff('{{ period_type }}', start_timestamp, stop_timestamp) / {{ period_length }} + 1 as num_periods,
                    {{ filter_columns | join(' ,') }}
                from data where {{ additional_filter_logic }}
                order by start_timestamp asc
            {% else %}
                select
                    format_datetime(start_timestamp, 'YYYY-MM-dd HH:mm:ss.SSS'),
                    format_datetime(stop_timestamp, 'YYYY-MM-dd HH:mm:ss.SSS'),
                    date_diff('{{ period_type }}', start_timestamp, stop_timestamp) / {{ period_length }} + 1 as num_periods
                from data
                order by start_timestamp asc
            {% endif %}

        {%- endset -%}

        {% call statement('boundary_period', fetch_result=True) -%}
            {{ boundary_sql }}
        {%- endcall %}

        {% set boundary_period = load_result('boundary_period') %}

        {% set period_boundaries = {
            'start_timestamp': boundary_period['data'][0][0] | string,
            'stop_timestamp': boundary_period['data'][0][1] | string,
            'num_periods': boundary_period['data'][0][2] | int
        } %}
    {% endif %}
    {% do return(period_boundaries) %}

{%- endmacro %}
