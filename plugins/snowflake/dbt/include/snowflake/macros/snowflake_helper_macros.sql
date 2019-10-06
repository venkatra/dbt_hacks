
/*
    cloning a table relation
*/
{% macro clone_table_relation_if_exists(old_relation ,clone_relation) %}
  {% if old_relation is not none %}
    {{ log("Cloning existing relation " ~ old_relation ~ " as a backup with name " ~ clone_relation) }}
    {% call statement('clone_relation', auto_begin=False) -%}
        CREATE OR REPLACE TABLE {{ clone_relation }}
            CLONE {{ old_relation }}
    {%- endcall %}
  {% endif %}
{% endmacro %}

/*
    Backing up (Copy of) a (transient) table relation
*/
{% macro copyof_table_relation_if_exists(old_relation ,clone_relation) %}
  {% if old_relation is not none %}
    {{ log("Copying of existing relation " ~ old_relation ~ " as a backup with name " ~ clone_relation) }}
    {% call statement('clone_relation', auto_begin=False) -%}
        CREATE OR REPLACE TABLE {{ clone_relation }}
            AS SELECT * FROM {{ old_relation }}
    {%- endcall %}
  {% endif %}
{% endmacro %}


{%- macro create_table_stmt_fromfile(relation, sql) -%}
    {{ log("Creating table " ~ relation) }}

    {{ sql.upper() }}
    ;

{%- endmacro -%}