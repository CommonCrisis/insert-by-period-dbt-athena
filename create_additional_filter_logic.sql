
{% macro create_additional_filter_logic(column_names, column_values) -%}
    {{ return(adapter.dispatch('create_additional_filter_logic', 'insert_by_period_v2')(column_names, column_values)) }}
{% endmacro %}


{% macro default__create_additional_filter_logic(column_names, column_values) -%}
    {%- set filter_logic = [] -%}

    {%- for i in range(column_names | length) -%}
        {%- set name = column_names[i] -%}

        {%- set value_list = [] -%}
        {%- for value in column_values[i] -%}
            {#- Check if the value is a string that needs to be quoted -#}
            {%- if value is string -%}
                {%- set value_list = value_list.append("'" ~ value | replace("'", "''") ~ "'") -%}
            {%- else -%}
                {%- set value_list = value_list.append(value) -%}
            {%- endif -%}
        {%- endfor -%}

        {%- set values = value_list | join(', ') -%}

        {%- set filter_logic = filter_logic.append(name ~ ' in ' ~ '(' ~ values ~ ')') -%}

    {%- endfor -%}

    {%- set result = filter_logic | join(' AND ') -%}

    {{- result -}}

{%- endmacro %}
