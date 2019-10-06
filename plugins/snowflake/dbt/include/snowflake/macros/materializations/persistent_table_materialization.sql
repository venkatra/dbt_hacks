/*
  This materialization is used for creating persistent table.
  The idea behind this materialization is for ability to define CREATE TABLE statements and have DBT the necessary logic
  of deploying the table in a consistent manner and logic. Some concepts have been borrowed from 'incremental' materialization:

   - https://github.com/fishtown-analytics/dbt/blob/0.14.latest/plugins/snowflake/dbt/include/snowflake/macros/materializations/incremental.sql

  Please read the markdown 'Persistent_Tables_Materialization.md' for a better reasoning behind this materialization.

*/
{% materialization persistent_table, adapter='snowflake' %}
    {%- set retain_previous_version_flg = config.get('retain_previous_version_flg', default=True) -%} -- indicate if the backup copy of previous version is to be retained.
    {%- set migrate_data_over_flg = config.get('migrate_data_over_flg', default=true) -%} -- indicate if the data needs to be migrated over to the newly defined table.

    {%- set unique_key = config.get('unique_key') -%}
     {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
    {%- set identifier = model['alias'] -%}

    {%- set current_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}

    {%- set backup_suffix_dt = py_current_timestring() -%}
    {%- set backup_table_suffix = config.get('backup_table_suffix', default='_DBT_BACKUP_') -%}
    {%- set backup_identifier = model['name'] + backup_table_suffix + backup_suffix_dt -%}
    {%- set backup_relation = api.Relation.create(database=database,
                                               schema=schema,
                                               identifier=backup_identifier,
                                               type='table') -%}

    {%- set target_relation = api.Relation.create(database=database,
                                               schema=schema,
                                               identifier=identifier,
                                               type='table') -%}
    {%- set tmp_relation = make_temp_relation(target_relation ,'_DBT_TMP') %}

    {%- set current_relation_exists_as_table = (current_relation is not none and current_relation.is_table) -%}
    {%- set current_relation_exists_as_view = (current_relation is not none and current_relation.is_view) -%}

    --------------------------------------------------------------------------------------------------------------------

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {# -- If the destination is a view, then we have no choice but to drop it #}
    {% if current_relation_exists_as_view %}
     {{ log("Dropping relation " ~ current_relation ~ " because it is a view and this model is a table.") }}
     {{ adapter.drop_relation(current_relation) }}
     {% set current_relation = none %}
    {% endif %}

    --------------------------------------------------------------------------------------------------------------------

     -- backup the existing table
    {% if current_relation_exists_as_table %}
        {{ clone_table_relation_if_exists(current_relation ,backup_relation) }}
    {% endif %}

    -- build model
    {% if full_refresh_mode or current_relation is none -%}
        -- drop the relation incase if the stmt happens to be CREATE IF NOT EXISTS
        {{ adapter.drop_relation(current_relation) }}

        {%- call statement('main') -%}
            {{ create_table_stmt_fromfile(target_relation, sql) }}
        {%- endcall -%}

        -- migrate the data over
        {% if migrate_data_over_flg and current_relation is not none %}
            {{ log("Migrating data from  " ~ backup_relation ~ " to " ~ target_relation) }}
            {% set dest_columns = adapter.get_columns_in_relation(backup_relation) %}
            {%- call statement('merge', fetch_result=False , auto_begin=False) -%}
               {{ get_merge_sql(target_relation, backup_relation, unique_key, dest_columns) }}
            {% endcall %}
        {%- endif %}

    {%- else -%}
        {%- call statement('main') -%}
            {% set tmpsql = sql.replace(identifier ,tmp_relation.identifier) %}
            {{ log("Tmp sql " ~ tmpsql) }}
            {{ create_table_stmt_fromfile(tmp_relation, tmpsql) }}

        {%- endcall -%}

        {%- set new_cols = adapter.get_missing_columns(tmp_relation, current_relation) %}
        {%- set dropped_cols = adapter.get_missing_columns(current_relation ,tmp_relation) %}

        {% if new_cols|length > 0 -%}
            -- CASE 1 : New columns were added
            -- https://docs.getdbt.com/docs/adapter#section-get_missing_columns
            {%- set new_cols_csv = new_cols | map(attribute="name") | join(', ') -%}
            {{ log("COL_ADDED : " ~ new_cols_csv )}}
            {% call statement('add_cols') %}
                {% for col in new_cols %}
                    alter table {{current_relation}} add column "{{col.name}}" {{col.data_type}};
                {% endfor %}
            {%- endcall %}
        {%- endif %}

        {% if dropped_cols|length > 0 -%}
            -- CASE 2 : Columns were dropped
            {%- set dropped_cols_csv = dropped_cols | map(attribute="name") | join(', ') -%}
            {{ log("COLUMNS TO BE DROPPED : " ~ dropped_cols_csv )}}
            {% call statement('drop_cols') %}
                {% for col in dropped_cols %}
                    alter table {{current_relation}} drop column "{{col.name}}";
                {% endfor %}
            {%- endcall %}
        {%- endif %}

        -- CASE 3 : Columns were renamed
        --  This is equivalent of dropped and renamed hence no additional logic needed

        -- CASE 4 : Column data type changed
        --  TODO identify and log if datatype are detected
        -- get_columns_in_relation
        -- alter_column_type
        -- https://github.com/fishtown-analytics/dbt/blob/f9c8442260e48bdd8bb7805b2e7541ab91492bb1/plugins/snowflake/dbt/include/snowflake/macros/adapters.sql
        {{ adapter.expand_target_column_types(from_relation=tmp_relation,
                                                    to_relation=current_relation) }}

         {{ adapter.drop_relation(tmp_relation) }}
    {%- endif %}


    {% if retain_previous_version_flg == False %}
        {{ adapter.drop_relation(backup_relation) }}
    {% endif %}

   --------------------------------------------------------------------------------------------------------------------

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation] ,'backup_relation': [backup_relation] }) }}

{%- endmaterialization %}