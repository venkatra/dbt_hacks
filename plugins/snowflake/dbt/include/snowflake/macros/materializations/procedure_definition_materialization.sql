/*
  This materialization is used for creating stage objects.
  The idea behind this materialization is for ability to define CREATE STAGE statements and have DBT the necessary logic
  of deploying the table in a consistent manner and logic.

*/
{% materialization procedure_definition, adapter='snowflake' %}
    {%- set full_refresh_mode = (flags.FULL_REFRESH == True) -%}
    {%- set identifier = model['alias'] -%}


    --------------------------------------------------------------------------------------------------------------------

    -- setup
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}


    --------------------------------------------------------------------------------------------------------------------

    -- build model
    {%- call statement('main') -%}
      {{ create_stmt_fromfile(sql) }}
    {%- endcall -%}



   --------------------------------------------------------------------------------------------------------------------

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'procedure': [identifier]  }) }}

{%- endmaterialization %}


