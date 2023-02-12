{%  set config = {
      "extract_type": "incremental",
      "incremental_column": "pull_date"
}%}

SELECT 
      pull_date,
      airport_code,
      "departures_onTime",
      departures_delayed,
      departures_canceled,
      "arrivals_onTime",
       arrivals_delayed,
       arrivals_canceled 
FROM 
      {{source_table}}
{% if is_incremental %}
WHERE pull_date > '{{ incremental_value }}'
{% endif %} 