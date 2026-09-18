{# dbt doesn't like us ref'ing in an operation so we fetch the info from the graph #}

{% macro upload_results(results) -%}

    {% if execute %}

        {% set datasets_to_load = [] %}
        {% if results != [] %}
            {# When executing, and results are available, then upload the results #}
            {% set datasets_to_load = ['test_executions'] %}
        {% endif %}

        {# Upload each data set in turn #}
        {% for dataset in datasets_to_load %}

            {% do log("Uploading " ~ dataset.replace("_", " "), true) %}

            {# Get the results that need to be uploaded #}
            {% set objects = dbt_artifacts.get_dataset_content(dataset) %}

            {# Upload in chunks to reduce the query size #}
            {% if dataset == 'models' %}
                {% set upload_limit = 50 if target.type == 'bigquery' else 100 %}
            {% else %}
                {% set upload_limit = 300 if target.type == 'bigquery' else 5000 %}
            {% endif %}

            {# Also cap each chunk by rendered SQL size: Athena rejects query strings over 262144 characters #}
            {% set max_query_chars = var('dbt_artifacts_max_query_chars', 200000) | int(200000) %}
            {% set ns = namespace(batch=[], size=0) %}

            {% for object in objects -%}

                {% set object_size = dbt_artifacts.get_table_content_values(dataset, [object]) | length %}

                {# Flush the current chunk before it goes over the row or size limit #}
                {% if ns.batch and (ns.batch | length >= upload_limit or ns.size + object_size > max_query_chars) %}
                    {{ dbt_artifacts.upload_results_chunk(dataset, ns.batch) }}
                    {% set ns.batch = [] %}
                    {% set ns.size = 0 %}
                {% endif %}

                {% set ns.batch = ns.batch + [object] %}
                {% set ns.size = ns.size + object_size %}

            {%- endfor %}

            {% if ns.batch %}
                {{ dbt_artifacts.upload_results_chunk(dataset, ns.batch) }}
            {% endif %}

        {# Loop the next 'dataset' #}
        {% endfor %}

    {% endif %}

{%- endmacro %}


{% macro upload_results_chunk(dataset, objects) -%}

    {# Insert the content into the metadata table #}
    {{ dbt_artifacts.insert_into_metadata_table(
        dataset=dataset,
        fields=dbt_artifacts.get_column_name_list(dataset),
        content=dbt_artifacts.get_table_content_values(dataset, objects)
        )
    }}

{%- endmacro %}
