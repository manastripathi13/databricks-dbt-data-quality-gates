-- Data quality scorecard: one row per run date and layer, from Great Expectations results.
select
    run_date,
    layer,
    count(*)                                                        as expectations_run,
    sum(case when success then 1 else 0 end)                        as expectations_passed,
    sum(case when not success and severity = 'critical' then 1 else 0 end) as critical_failures,
    sum(case when not success and severity = 'warning' then 1 else 0 end)  as warnings,
    round(100.0 * sum(case when success then 1 else 0 end) / count(*), 1) as pass_rate_pct,
    sum(case when not success and severity = 'critical' then 1 else 0 end) = 0 as met_quality_bar
from (
    select *, row_number() over (partition by run_date, layer, table_name, expectation, column_name
                                 order by validated_at desc) as rn
    from {{ source('dq', 'validation_results') }}
) latest
where rn = 1
group by 1, 2
