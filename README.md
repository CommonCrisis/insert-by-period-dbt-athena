Macro is based on: https://gist.github.com/jessedobbelaere/6fdb593f9e2cc732e9f142c56c9bac87


This macro allows to build incremental models by splitting the source table into smaller chunks. This is handy if the source table is really really big and processing would take longer than 30 minutes.
In a nutshell the macro splits the source table in a for loop into chunks by `timestamp_field`.
In version 2 you can add multiple filters to further reduce the amount of source data.

Make sure your incremental dbt model has the following structure:

```sql
{{ config block }}

with model as (
  select * from source
  where __period_filter__
)
```

`__period_filter__` will be replaced with your config logic by the macro.

The **parameters** to set in the config block are:

- **materialized**: 'insert_by_period_v2'
- **table_type**: 'iceberg' or 'hive'
- **incremental_strategy**: [See athena dbt adapter](https://github.com/dbt-athena/dbt-athena)
- **timestamp_field**: This is the column the macro will split the source table into chunks
- **src_table_timestamp_field**: *optional- if your source table for your incremental model has another name for the `timestamp_field` you can provide it here
- **is_timestamp_string**: If this column is not datetime or date type but varchar set it to `true`
- **start_date¹**: The first date the processing will be executed on
- **days_before_today¹**: Here you can provide a number as string. The `start_date` will then be calculated by `today - days_before_today` as date. This will overwrite `start_date`
- **stop_date¹**: *optional- A due date for the processing - here it will stop chunking the source table. If you set this `force_dates` will be enabled and only dates between `start_date` and `stop_date` will be processed
- **period¹**: The chunk size - [see interval functions](https://trino.io/docs/current/functions/datetime.html#interval-functions)
- **unique_key**: *optional- [See athena dbt adapter](https://github.com/dbt-athena/dbt-athena)
- **partitioned_by**: *optional- partitions columns of target table - [see athena dbt adapter](https://github.com/dbt-athena/dbt-athena)
- **target_filter_column_names¹**: *optional- list of column names you want to filter on the target (e.g. `["project_id", "creation_date"]`)
- **source_filter_column_names¹**: *optional- list of column names you want to filter on the source (e.g. `["project_key", "creation_date"]`)
- **filter_column_values¹**: *optional- list of lists of the values you want to filter on. Make sure you provide them with the correct data type (e.g. `[[1, 5, 99], ["2023-06-08"]]`)
- **on_schema_change**: [See athena dbt adapter](https://github.com/dbt-athena/dbt-athena)
- **s3_data_naming**: [See athena dbt adapter](https://github.com/dbt-athena/dbt-athena)
- **post_hook**: [See athena dbt adapter](https://github.com/dbt-athena/dbt-athena)
- **table_properties**: [See athena dbt adapter](https://github.com/dbt-athena/dbt-athena)

¹: These most of the time follow this logic: If not provided as variable --> check the configuration of the model. If not variable or config block --> fall back to the default value in the macro.

```sql
{{
  config(
    materialized = 'insert_by_period_v2',
    table_type='iceberg',
    incremental_strategy='merge',
    timestamp_field = 'creation_date',
    src_table_timestamp_field = 'creation_date',
    is_timestamp_string = False,
    start_date = var('start_date', '2023-01-01'),
    period = var('period', '15 day'),
    target_filter_column_names = var('target_filter_column_names', ["project_id"]), -- you should not do this but provide the filters with the --vars parameter
    source_filter_column_names = var('source_filter_column_names', ["project_key"]), -- you should not do this but provide the filters with the --vars parameter
    filter_column_values = var('filter_column_values', [[99]]), -- you should not do this but provide the filters with the --vars parameter
    unique_key=['creation_date', 'project_id', 'some_surrogate_key'],
    partitioned_by=['project_id', 'month(creation_date)'],
    on_schema_change='append_new_columns',
    s3_data_naming = 'schema_table_unique',
    post_hook=['optimize {{ this }} rewrite data using bin_pack', 'vacuum {{ this }}']
  )
}}

```

If `var(...)` is set it actually not meant to be hardcoded by the parameters should be provided in you dbt command:

`dbt run -s my_model --vars '{"start_date": "2024-05-30", "stop_date": "2024-06-03", "filter_column_values": [[77], [3, 4, 6]], "target_filter_column_names": ["project_id"], "source_filter_column_names": ["project_key"]}'`

You can check out the [--vars statement here](https://docs.getdbt.com/docs/build/project-variables).
