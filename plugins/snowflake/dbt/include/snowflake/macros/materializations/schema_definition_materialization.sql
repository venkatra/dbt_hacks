

{% materialization schema_definition, adapter='snowflake' %}
    {%- set retain_previous_version_flg = config.get('retain_previous_version_flg', default=True) -%} -- indicate if the backup copy of previous version is to be retained.
    {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
    {%- set identifier = model['alias'] -%}


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
      {{ log("Creating schema " ~ schema) }}
      {{ create_stmt_fromfile(sql) }}
    {%- endcall -%}



   --------------------------------------------------------------------------------------------------------------------

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'schema': [identifier]  }) }}

{%- endmaterialization %}
