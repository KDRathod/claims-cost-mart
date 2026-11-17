{#
  One place that maps raw encounter classes to the service categories finance
  reports on. Lag behaviour differs by category (a hospital stay takes far
  longer to settle than an office visit), so completion factors use it too.
#}
{% macro service_category(encounter_class) -%}
    case
        when {{ encounter_class }} in ('inpatient', 'snf', 'hospice') then 'facility_stay'
        when {{ encounter_class }} in ('emergency', 'urgentcare')      then 'emergency_urgent'
        when {{ encounter_class }} in ('outpatient', 'ambulatory')     then 'outpatient'
        when {{ encounter_class }} in ('wellness', 'virtual', 'home')  then 'primary_preventive'
        else 'other'
    end
{%- endmacro %}
