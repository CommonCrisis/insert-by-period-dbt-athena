-- This is more or less 1:1 copy from the dbt athena adapter logic. So if sth is not working anymore after an update this may need some adjustments

{% macro period_build_iceberg(tmp_relation, on_schema_change, target_relation, existing_relation, force_batch) -%}
    {{ return(adapter.dispatch('period_build_iceberg', 'insert_by_period_v2')(tmp_relation, on_schema_change, target_relation, existing_relation, force_batch)) }}
{% endmacro %}


{% macro default__period_build_iceberg(tmp_relation, on_schema_change, target_relation, existing_relation, force_batch) -%}
    {{ dbt_utils.log_info("Using incremental logic for Iceberg tables...") }}

    {% set unique_key = config.get('unique_key') %}
    {% set incremental_predicates = config.get('incremental_predicates') %}
    {% set delete_condition = config.get('delete_condition') %}
    {% set update_condition = config.get('update_condition') %}
    {% set insert_condition = config.get('insert_condition') %}

    {% set empty_unique_key -%}
        Merge strategy must implement unique_key as a single column or a list of columns.
    {%- endset %}

    {% if unique_key is none %}
        {% do exceptions.raise_compiler_error(empty_unique_key) %}
    {% endif %}

    {% if incremental_predicates is not none %}
        {% set inc_predicates_not_list -%}
            Merge strategy must implement incremental_predicates as a list of predicates.
        {%- endset %}
        {% if not adapter.is_list(incremental_predicates) %}
            {% do exceptions.raise_compiler_error(inc_predicates_not_list) %}
        {% endif %}
    {% endif %}

    {% set query_result = safe_create_table_as(True, tmp_relation, compiled_code, model_language, force_batch) -%}

    {% set build_sql = iceberg_merge(
        on_schema_change=on_schema_change,
        tmp_relation=tmp_relation,
        target_relation=target_relation,
        unique_key=unique_key,
        incremental_predicates=incremental_predicates,
        existing_relation=existing_relation,
        delete_condition=delete_condition,
        update_condition=update_condition,
        insert_condition=insert_condition,
        force_batch=force_batch,
    )
    %}

    {{- build_sql -}}

{%- endmacro %}
