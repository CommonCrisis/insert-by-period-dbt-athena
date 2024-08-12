{#
    This is an Athena compatible version of the Redshift materialization found
    in https://github.com/dbt-labs/dbt-labs-experimental-features/blob/main/insert_by_period
#}

{% materialization insert_by_period_v2, adapter='athena' -%}
    {%- set table_type = config.require('table_type') -%}

    {%- set timestamp_field = config.require('timestamp_field') -%}
    {%- set start_date = var('start_date', config.require('start_date')) -%}
    {%- set days_before_today = var('days_before_today', default=config.get('days_before_today', default='')) -%}

    -- Dynamically get the start date by goging n days backwards from today
    {% if days_before_today %}
        {{ dbt_utils.log_info('days_before_today is set - overwriting start_date: ' ~ start_date) }}
        {% set start_date = adapter.dispatch('generate_start_date', 'insert_by_period_v2')(
            days_before_today=days_before_today
            )
        %}
    {% endif %}

    {%- set stop_date = var('stop_date', config.get('stop_date', default='')) -%}
    {%- set force_dates = (var('start_date', '') != '' or var('stop_date', '') != '' or var('days_before_today', '') != '') -%}
    {%- set src_table_timestamp_field = config.get('src_table_timestamp_field', default=config.get('timestamp_field')) -%}
    {%- set force_batch = config.get('force_batch', False) | as_bool -%}

    {%- if force_dates -%}
      {{ dbt_utils.log_info('Forcing dates between ' ~ start_date ~ ' and ' ~ (stop_date if stop_date else 'today')) }}
    {%- endif -%}

    {%- set period = config.get('period', default='10 day') -%}
    {%- set is_timestamp_string =  config.get('is_timestamp_string', default=True) -%}
    {%- set timestamp_format = config.get('timestamp_format', default='%Y-%m-%d') -%}

    -- Additonal filters
    -- This is used to further filter down the the source table
    -- Source and target table could have renames in their columns so we need to define target and source column names
    {%- set target_filter_column_names = var('target_filter_column_names', default=config.get('target_filter_column_names', default=[])) -%}
    {%- set source_filter_column_names = var('source_filter_column_names', default=config.get('source_filter_column_names', default=target_filter_column_names)) -%}

    -- Check if src and target columns have same size
    {% if target_filter_column_names|length != source_filter_column_names|length %}
        {%- set error_message -%}
            Model '{{ model.unique_id }}' needs same amount of target_filter_column_names and source_filter_column_names
        {%- endset -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {% endif %}

    {%- set filter_column_values = var('filter_column_values', default=config.get('filter_column_values', default=[])) -%}

    -- Check if values and columns are same length
    {% if target_filter_column_names|length != filter_column_values|length %}
        {%- set error_message -%}
            Model '{{ model.unique_id }}' needs same amount of target_filter_column_names and filter_column_values
        {%- endset -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {% endif %}

    -- Dbt Athena adapter configs
    {%- set partitioned_by = config.get('partitioned_by', default=none) -%}
    {%- set full_refresh_mode = (should_full_refresh()) -%}
    {%- if sql.find('__period_filter__') == -1 -%}
        {%- set error_message -%}
            Model '{{ model.unique_id }}' does not include the required string '__period_filter__' in its sql
        {%- endset -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}
    {%- set existing_relation = load_relation(this) -%}
    {%- set target_relation = this.incorporate(type='table') -%}
    {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}

    -- Creating the table if not exists or deleting it first if it already exists in full refresh mode or is existent as view.
    {%- set sql_without_where_clause = sql | replace("__period_filter__", 'false') -%}
    {% if existing_relation is none -%}
        {{ dbt_utils.log_info('Target relation ' ~ target_relation ~ ' does not exist, yet. Creating...') }}
        {% call statement('main') -%}
            {{ create_table_as(False, target_relation, sql_without_where_clause) }}
        {%- endcall %}
    {%- elif existing_relation.is_view or full_refresh_mode -%}
        {{ dbt_utils.log_info('Target relation ' ~ target_relation ~ ' is a view or full refresh. Dropping and recreating as table...') }}
        {{ adapter.drop_relation(existing_relation) }}
        {% call statement('main') -%}
            {{ create_table_as(False, target_relation, sql_without_where_clause) }}
        {%- endcall %}
    {%- endif %}

    -- Here if the table did not exist or we were in full refresh mode existing_relation would be empty even if it had been recreated
    -- That crashed the incremental insert
    -- Maybe this will cause issues but I just assign the variable to the newly created table
    {%- set existing_relation = this -%}

    -- build model
    {{ run_hooks(pre_hooks, inside_transaction=False)}}
    {{ run_hooks(pre_hooks, inside_transaction=True)}}
    {%- set target_table_name = target_relation.name -%}

    {% set additional_filter_logic = adapter.dispatch('create_additional_filter_logic', 'insert_by_period_v2')(
        column_names=target_filter_column_names,
        column_values=filter_column_values
        )
    %}

    {% if additional_filter_logic -%}
        {% do dbt_utils.log_info("Using target filter: " ~ additional_filter_logic ~ " ...") %}
    {% else %}
        {% do dbt_utils.log_info("No additonal filter provided") %}
    {% endif %}

    {% set period_boundaries = adapter.dispatch('get_period_boundaries_v2', 'insert_by_period_v2')(schema,
                                target_table_name,
                                timestamp_field,
                                start_date,
                                stop_date,
                                force_dates,
                                period,
                                is_timestamp_string,
                                timestamp_format,
                                additional_filter_logic,
                                target_filter_column_names) %}

    {%- set start_timestamp = period_boundaries['start_timestamp'] -%}
    {%- set stop_timestamp = period_boundaries['stop_timestamp'] -%}
    {%- set num_periods = period_boundaries['num_periods'] -%}
    {% set target_columns = adapter.get_columns_in_relation(target_relation) %}
    {%- set target_cols_csv = target_columns | map(attribute='quoted') | join(', ') -%}

    {% for i in range(num_periods) -%}
        {% do dbt_utils.log_info("Running for batch [" ~ (i+1) ~ "/" ~ (num_periods) ~ "] of " ~ period ~ "s length") %}

        {% do dbt_utils.log_info("Creating temp relation " ~ tmp_relation ~ " ...") %}

        {% if not additional_filter_logic %}
            {% set tmp_relation = make_temp_relation(target_relation, '__inc_period_dbt_tmp') %}
        {% else %}
            -- Creating a hash based on the unique filter logic to avoid having duplicate temp relations with the same name
            {%- set value_hash = local_md5(additional_filter_logic) -%}
            {%- set temp_relation_suffix = '__inc_period_dbt_tmp_' + value_hash -%}
            {%- set tmp_relation = make_temp_relation(target_relation, temp_relation_suffix) -%}
        {% endif %}

        {% if tmp_relation is not none %}
            {% do dbt_utils.log_info("Dropping old data from " ~ tmp_relation ~ " ...") %}
            {% do drop_relation(tmp_relation) %}
        {% endif %}

        {% set additional_filter_logic = adapter.dispatch('create_additional_filter_logic', 'insert_by_period_v2')(
            column_names=source_filter_column_names,
            column_values=filter_column_values
            )
        %}

        {% if additional_filter_logic %}
            {% do dbt_utils.log_info("Using source filter: " ~ additional_filter_logic ~ " ...") %}
        {% endif %}

        {# Setting the temp tabel to copy from as sql command #}
        {% set tmp_table_sql = adapter.dispatch('get_period_sql_v2', 'insert_by_period_v2')(target_cols_csv=target_cols_csv,
                                sql=sql,
                                timestamp_field=src_table_timestamp_field,
                                start_timestamp=start_timestamp,
                                stop_timestamp=stop_timestamp,
                                period=period,
                                is_timestamp_string=is_timestamp_string,
                                timestamp_format=timestamp_format,
                                offset=i,
                                additional_filter_logic=additional_filter_logic) %}

        {# Executing the creation of the temp table with batches forced #}
        {% set query_result = safe_create_table_as(temporary=True, relation=tmp_relation, compiled_code=tmp_table_sql, language='sql', force_batch=force_batch) -%}

        {# Check if temp rleation has columns. If not it does not exist - not ideal but checking the raltion doesn't work... #}
        {%- set columns = adapter.get_columns_in_relation(tmp_relation) -%}
        {%- set temp_table_check = adapter.get_relation(schema=schema, identifier=tmp_relation.identifier, database=database) -%}

        {% if columns|length < 1 %}
            {{ dbt_utils.log_info("Batch [" ~ (i + 1) ~ "/" ~ (num_periods) ~ "] has not data. We are skipping ...") }}
            {#  Hacky but it works  #}
            {% call statement("main") %}
                Select ''
            {% endcall %}

        {% else %}
            {% if table_type == 'iceberg' %}
                {% set build_sql = adapter.dispatch('period_build_iceberg', 'insert_by_period_v2')(
                    tmp_relation=tmp_relation,
                    on_schema_change=on_schema_change,
                    target_relation=target_relation,
                    existing_relation=existing_relation,
                    force_batch=force_batch
                ) %}

                {% call statement("main") %}
                    {{ build_sql }}
                {% endcall %}

            -- Running on hive
            {% else %}
                -- Delete overlapping partitions between temp and target
                {{ dbt_utils.log_info("Delete overlapping partitions...") }}
                {% do delete_overlapping_partitions(target_relation=target_relation, tmp_relation=tmp_relation, partitioned_by=partitioned_by) %}

                {{ dbt_utils.log_info("Do the whole background batching stuff and insert into target relation...") }}

                -- Running main cause otherwise we get an error at the end
                {% call statement("main") %}
                    {{ incremental_insert(
                        on_schema_change, tmp_relation, target_relation, existing_relation, force_batch
                    ) }}
                {% endcall %}

            {% endif %}

        {% endif %}

        {{ dbt_utils.log_info("Dropping temp relation...") }}
        {% do adapter.drop_relation(tmp_relation) %}

        {{ dbt_utils.log_info("Finished period of " ~ period ~ " [" ~ (i + 1) ~ "/" ~ (num_periods) ~ "]") }}
    {%- endfor %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {% do adapter.commit() %}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}
