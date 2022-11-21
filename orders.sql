*/
About the data:

In this example we have several companies aka 'brands'.
Each brand has their own datasets and each data set has their own columns.
Their dataset and columns have the same schema but <<not all of them are present!!>>

Thus, this script will only use the datasets AND collumns present to avoid errors.

For this example we will use an example using shopify informations

Process:
1) Get all companies(brands) dataset names
2) Define the tables that will be use in a list.
3) Run final query with common tables and dynamic tables (by using the dictionnary information)
*/


--------------------------------------------
--Get all companies(brands) dataset names --
--------------------------------------------

--Create a query to get the names
{% set get_brands_dataset_names_query %}
SELECT
    DISTINCT schema_name
FROM
`datacluster.INFORMATION_SCHEMA.SCHEMATA`
WHERE
schema_name LIKE "shopify_%"
{%- endset -%}


--Run query and add result to a list (brands_list)
{%- set dataset_query = run_query(get_brands_dataset_names_query) -%}
{#"This is necessary to avoid errors on compilation time"#}
{%- if execute -%}
{%- set brands_list = dataset_query.columns[0].values() -%}
{%- else -%}
{%- set brands_list = [] -%}
{%- endif -%}

-- Create a dictionary
{%- set brand_tables_dict = { } -%}
--Get the brands TABLES name
{%- for brand in brands_list -%}
    --Create Query to get each company table name
    {%- set brands_tables_query -%}
        SELECT
        DISTINCT table_id
        FROM
        `cluster_data.{{brand}}.__TABLES__`
    {%- endset -%}
    --Run query and add result to a list (tables_query)
    {%- set tables_query = run_query(brands_tables_query) -%}
    {#"This is necessary to avoid errors on compilation time"#}
    {%- if execute -%}
        {%- set tables_list = tables_query.columns[0].values() -%}
    {%- else -%}
        {%- set tables_list = [] -%}
    {%- endif -%}
    --Add both results (brand + tables_list)to the dictionnary (brand_table_dict)
    {%- do  brand_tables_dict.update({brand: tables_list}) -%}
{%endfor%}

------------------------------------------------------------
---- 2) Define the tables that will be use in a list.   ----
------------------------------------------------------------

{% set order_shipping_line_list = ["order_shipping_line_discounted_price", "order_shipping_line_price" ] %}


---------------------------------------------------------------------------------------------------------
---- 3) Run final query with common tables and dynamic tables (by using the dictionnary information) ----
---------------------------------------------------------------------------------------------------------

WITH

order_conversion_base AS  --<< common table
(
    SELECT
        brand
        ,seller_table
        ,purchase_timestamp_utc
        ,order_id
        ,currency_conversion_rate_utc
        ,currency_conversion_rate_nonutc
    FROM
        {{ ref('shopify_orders_pre_base_core') }}
)

------------------------------------------------------------
----------  order_discount_code (uncomon table)    ---------
------------------------------------------------------------

, order_discount_code AS (

{% for brand in brand_tables_dict.keys() %} --<<it will go brand by brand
{%- if 'order_discount_code' in brand_tables_dict[brand] -%} --<< if the brand has this table it will use it
{% if not loop.first %} UNION ALL {% endif %} --<< finally, it will union all the brands.
    SELECT
        "{{brand}}" as seller_table
        ,index AS order_discount_code_index
        ,order_id
        ,amount AS order_discount_code_amount
        ,code AS order_discount_code
        ,type AS order_discount_code_type
    FROM
        `cluster_data.{{brand}}.order_discount_code`

{%endif%}
{%endfor%}
)

,converted_order_discount_code_base AS (
   SELECT
        --Order --18
        b.brand
        ,b.purchase_timestamp_utc
        ,b.order_id

        ,ARRAY_AGG(STRUCT(
            a.order_discount_code_index
            ,a.order_discount_code_amount
            ,a.order_discount_code
            ,a.order_discount_code_type)
        ) AS order_discount_code_array
    FROM
        order_discount_code AS a
    LEFT JOIN
        order_conversion_base AS b
        ON a.seller_table = b.seller_table
        AND a.order_id = b.order_id

    GROUP BY 1, 2, 3
)


-----------------------------------------------------
----------  order_shipping_line  (uncomon table) ----
-----------------------------------------------------

,order_shipping_line AS (

{% for brand in brand_tables_dict.keys() %}
{%- if 'order_shipping_line' in brand_tables_dict[brand] -%}
{% if not loop.first %} UNION ALL {% endif %}
    SELECT
        "{{brand}}" as seller_table
        ,id AS order_shipping_line_id
        ,order_id
        ,carrier_identifier AS order_shipping_line_carrier_identifier
        ,code AS order_shipping_line_code
        ,delivery_category AS order_shipping_line_delivery_category
        ,CAST(JSON_VALUE(discounted_price_set,
                            "$.shop_money.amount") AS numeric) AS order_shipping_line_discounted_price --noqa
        ,JSON_VALUE(discounted_price_set,
                            "$.shop_money.currency_code") AS order_shipping_line_discounted_currency  --noqa
        ,CAST(JSON_VALUE(price_set,
                            "$.shop_money.amount") AS numeric) AS order_shipping_line_price --noqa
        ,JSON_VALUE(price_set,
                            "$.shop_money.currency_code") AS order_shipping_line_currency  --noqa
        ,requested_fulfillment_service_id AS order_shipping_line_requested_fulfillment_service_id
        ,source AS order_shipping_line_source
        ,title AS order_shipping_line_title
    FROM
        `cluster_data.{{brand}}.order_shipping_line` 

{%endif%}
{%endfor%}
)

,converted_order_shipping_line_base AS (
   SELECT
        --Order --18
        b.brand
        ,b.purchase_timestamp_utc
        ,b.order_id
        --order_shipping_line
        ,ARRAY_AGG(STRUCT(
            a.order_shipping_line_id
            ,a.order_shipping_line_carrier_identifier
            ,a.order_shipping_line_code
            ,a.order_shipping_line_delivery_category
            ,a.order_shipping_line_discounted_price
            ,a.order_shipping_line_discounted_currency
            ,a.order_shipping_line_price
            ,a.order_shipping_line_currency
            ,a.order_shipping_line_requested_fulfillment_service_id
            ,a.order_shipping_line_source
            ,a.order_shipping_line_title

            --currency converted
            {% for item in order_shipping_line_list %}
            ,(a.{{item}} * currency_conversion_rate_utc
            ) AS {{item}}_usd_utc
            ,(a.{{item}} * currency_conversion_rate_nonutc
            ) AS {{item}}_usd_nonutc
            {%endfor%}
            )) AS order_shipping_line_array
    FROM
        order_shipping_line AS a
    LEFT JOIN
        order_conversion_base AS b
        ON a.seller_table = b.seller_table
        AND a.order_id = b.order_id

    GROUP BY 1, 2, 3
)

---------------------------
---------------------------

SELECT
         a.*
        ,b.* EXCEPT (brand, purchase_timestamp_utc, order_id)
        ,c.* EXCEPT (brand, purchase_timestamp_utc, order_id)
    FROM
        {{ ref('shopify_orders_pre_base_core') }} AS a

    LEFT JOIN
        converted_order_discount_code_base AS b
    ON a.brand = b.brand
    AND a.purchase_timestamp_utc = b.purchase_timestamp_utc
    AND a.order_id = b.order_id

    LEFT JOIN
        converted_order_shipping_line_base AS c
    ON a.brand = c.brand
    AND a.purchase_timestamp_utc = c.purchase_timestamp_utc
    AND a.order_id = c.order_id
