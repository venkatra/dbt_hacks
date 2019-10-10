{{
    config(materialized='persistent_table'
        ,retain_previous_version_flg=false
        ,migrate_data_over_flg=false
        ,enabled=true

    )
}}

CREATE OR REPLACE TABLE "{{ database }}"."{{ schema }}"."ADDRESS" (
	STREET_NUMBER   NUMBER(4),
	LINE1 VARCHAR(200),
	LINE2 VARCHAR(200),
	CITY VARCHAR(200),
	STATE VARCHAR(200),
	ZIPCODE VARCHAR(100),
	COUNTRY VARCHAR(100)
)

