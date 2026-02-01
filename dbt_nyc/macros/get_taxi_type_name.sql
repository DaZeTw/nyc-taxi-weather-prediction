{#
    This macro returns the description of the taxi type 
#}

{% macro get_taxi_type_name(taxi_type) -%}

    case {{ taxi_type }}
        when 'yellow' then 'Yellow Taxi'
        when 'green' then 'Green Taxi'
        when 'fhv' then 'For-Hire Vehicle'
        when 'fhvhv' then 'High Volume For-Hire Vehicle'
        else 'Unknown'
    end

{%- endmacro %}