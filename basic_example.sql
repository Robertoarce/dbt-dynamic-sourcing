*/
About the data: PLEASE Do not try to understand the exchanges in data, but rather the DBT usage!!

In this example we have several companies aka 'entity'.
Each entity has their own datasets and each data set has their own columns.
Their dataset and columns have the same schema but <<not all of them are present!!>>

Thus, this script will only use the datasets AND collumns present to avoid errors.

For this example we will use an example using shopify informations

Process:
1) Get all companies(entity) dataset names
2) Define the tables that will be use in a list.
3) Run final query with common tables and dynamic tables (by using the dictionnary information)
*/


--------------------------------------------
--Get all entities (companies) dataset names --
--------------------------------------------

--Create a query to get the names
{% set get_entity_dataset_names_query %}
SELECT
    DISTINCT schema_name
FROM
`datacluster.INFORMATION_SCHEMA.SCHEMATA`
WHERE
schema_name LIKE "shopify_%"
{%- endset -%}


--Run query and add result to a list (entity_list)
{%- set dataset_query = run_query(get_entity_dataset_names_query) -%}
{#"This is necessary to avoid errors on compilation time"#}
{%- if execute -%}
{%- set entity_list = dataset_query.columns[0].values() -%}
{%- else -%}
{%- set entity_list = [] -%}
{%- endif -%}

-- Create a dictionary
{%- set entity_tables_dict = { } -%}
--Get the entity TABLES name
{%- for entity in entity_list -%}
    --Create Query to get each company table name
    {%- set entity_tables_query -%}
        SELECT
        DISTINCT table_id
        FROM
        `cluster_data.{{entity}}.__TABLES__`
    {%- endset -%}
    --Run query and add result to a list (tables_query)
    {%- set tables_query = run_query(entity_tables_query) -%}
    {#"This is necessary to avoid errors on compilation time"#}
    {%- if execute -%}
        {%- set tables_list = tables_query.columns[0].values() -%}
    {%- else -%}
        {%- set tables_list = [] -%}
    {%- endif -%}
    --Add both results (entity + tables_list)to the dictionnary (entity_table_dict)
    {%- do  entity_tables_dict.update({entity: tables_list}) -%}
{%endfor%}

------------------------------------------------------------
---- 2) Define the tables that will be use in a list.   ----
------------------------------------------------------------

{% set order_shipping_line_list = ["order_shipping_line_discounted_price", "order_shipping_line_price" ] %}


---------------------------------------------------------------------------------------------------------
---- 3) Run final query with common tables and dynamic tables (by using the dictionnary information) ----
---------------------------------------------------------------------------------------------------------

WITH

sales_dataset AS (

{% for dataset in entity_tables_dict.keys() %} --<<it will go through each dataset in the entity
{%- if 'historic_sales' in entity_tables_dict[entity] -%} --<< if the entity has this table it will use it
{% if not loop.first %} UNION ALL {% endif %} --<< if not pass ignore and union next entity table.
    SELECT
        "{{entity}}" as entity_name --<< This will be our marker for the entity
        ,id AS sales_id
        ,order as sales_order_id
        ,salesman_id
        ,amount AS sales_amount
        ,code AS sales_product_code
        ,product_type AS sales_product_type
    FROM
        `cluster_data.{{entity}}.historic_sales`

{%endif%}
{%endfor%}
)

,supply_chain_dataset AS (

{% for dataset in entity_tables_dict.keys() %} --<<it will go through each dataset in the entity
{%- if 'supply_chain' in entity_tables_dict[entity] -%} --<< if the entity has this table it will use it
{% if not loop.first %} UNION ALL {% endif %} --<< if not pass ignore and union next entity table.
    SELECT
        "{{entity}}" as entity_name --<< This will be our marker for the entity
        ,warehouse_id
        ,order_id AS supply_chain_order_id
        ,origin_country
        ,arrival_country
        ,amount AS transfer_amount
        ,code AS product_code
        ,product_type
    FROM
        `cluster_data.{{entity}}.supply_chain`

{%endif%}
{%endfor%}
)

,crm_dataset AS (

{% for dataset in entity_tables_dict.keys() %} --<<it will go through each dataset in the entity
{%- if 'crm' in entity_tables_dict[entity] -%} --<< if the entity has this table it will use it
{% if not loop.first %} UNION ALL {% endif %} --<< if not pass ignore and union next entity table.
    SELECT
        "{{entity}}" as entity_name --<< This will be our marker for the entity
        ,message_id
        ,order_id AS crm_order_id
        ,user_id
        ,campaign_id
        ,is_premium_user
        ,message_timestamp_utc
        ,message_timestamp_est
    FROM
        `cluster_data.{{entity}}.crm`

{%endif%}
{%endfor%}
)
----------------------------------
---- Putting all in one table ----
----------------------------------

SELECT
         a.*
        ,b.* EXCEPT (product_type)
        ,c.* EXCEPT (message_timestamp_est)
    FROM
        sales_dataset AS a

    LEFT JOIN
        supply_chain AS b
    ON a.entity_name = b.entity_name 
    AND a.sales_order_id = b.supply_chain_order_id

    LEFT JOIN
        crm AS c
    ON a.entity_name = c.entity_name 
    AND a.sales_order_id = c.crm_order_id
