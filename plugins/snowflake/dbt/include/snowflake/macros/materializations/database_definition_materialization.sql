

{% materialization database_definition, adapter='snowflake' %}
    {%- set retain_previous_version_flg = config.get('retain_previous_version_flg', default=True) -%} -- indicate if the backup copy of previous version is to be retained.
    {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
    {%- set identifier = model['alias'] -%}

    {%- set current_relation = adapter.get_relation(database=database, schema='public', identifier=identifier) -%}
    {%- set target_relation = api.Relation.create(database=database,
                                               schema='information_schema',
                                               identifier='tables',
                                               type='table') -%}

    --------------------------------------------------------------------------------------------------------------------

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}


    --------------------------------------------------------------------------------------------------------------------

    {% if retain_previous_version_flg == False %}
        -- TODO: drop previous schema
    {%- else -%}
        -- TODO: backup the existing schema
    {% endif %}

    -- build model
    {%- call statement('main') -%}
      {{ log("Creating database  " ~ database) }}

      {{ create_stmt_fromfile(sql) }}
    {%- endcall -%}

   --------------------------------------------------------------------------------------------------------------------

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]  }) }}

{%- endmaterialization %}


