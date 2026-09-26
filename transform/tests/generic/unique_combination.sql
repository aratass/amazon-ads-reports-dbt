{#- Fails for every combination of `columns` that occurs more than once in the model. -#}
{% test unique_combination(model, columns) %}

select
    {{ columns | join(', ') }},
    count(*) as occurrences
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1

{% endtest %}
