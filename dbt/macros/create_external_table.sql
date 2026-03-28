{% macro create_yfin_external_table() %}

    {% set query %}
        CREATE EXTERNAL TABLE IF NOT EXISTS `{{ env_var('GCP_PROJECT_ID') }}.stock_data_raw.yfin_stock_ticks_ext`
        WITH PARTITION COLUMNS (
            stock_symbol STRING,
            data_type_label STRING
        )
        OPTIONS (
            format = 'CSV',
            uris = ['gs://{{ env_var('GCS_BUCKET_NAME') }}/yfin_raw_data/*.csv'],
            hive_partition_uri_prefix = 'gs://{{ env_var('GCS_BUCKET_NAME') }}/yfin_raw_data/',
            skip_leading_rows = 1,
            require_hive_partition_filter = FALSE
        );
    {% endset %}

    {% do run_query(query) %}
    {{ log("Successfully refreshed yfin_stock_ticks_ext", info=True) }}

{% endmacro %}