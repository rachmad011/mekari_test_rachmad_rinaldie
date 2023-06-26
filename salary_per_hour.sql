with extract_all_data_detail as (
  select 
    * 
    , (timestamp_diff(tt.checkout_ts, tt.checkin_ts, HOUR)) work_duration
  from
  (select
    ts.timesheet_id
    , ts.employee_id
    , ts.date
    , ee.branch_id
    , ee.salary
    , cast(concat(ts.date, ' ',ts.checkin) as TIMESTAMP) checkin_ts
    , cast(concat(ts.date, ' ',ts.checkout) as TIMESTAMP) checkout_ts
    , extract(MONTH from ts.date) month
    , extract(YEAR from ts.date) year
  from `spheric-gearing-318714.rachmadrinaldie_dataset.timesheets` ts
  left join `spheric-gearing-318714.rachmadrinaldie_dataset.employees` ee on ts.employee_id = ee.employee_id
  where ts.checkin is not null and ts.checkout is not null and (ee.resign_date is null or ee.resign_date < ts.date)
  order by ts.timesheet_id asc, ee.branch_id asc, ee.employee_id asc) tt
  where (timestamp_diff(tt.checkout_ts, tt.checkin_ts, HOUR)) > 0
)

, get_total_hour_employee_monthly as (
  select
    ex.year
    , ex.month
    , ex.branch_id
    , ex.employee_id
    , ex.salary
    , sum(work_duration) total_work_hour
  from extract_all_data_detail ex
  where ex.checkin_ts is not null and ex.checkout_ts is not null
  group by ex.year, ex.month, ex.branch_id, ex.salary, ex.employee_id
  order by 1,2,3 asc
)

, get_total_salary_per_branch_monthly as (
  select
    gs.year
    , gs.month
    , gs.branch_id
    , sum(gs.total_salary) total_salary_branch
    , sum(gs.total_work_hour) total_work_hour_branch
  from
  (select
    em.year
    , em.month
    , em.branch_id
    , sum(em.salary) total_salary
    , em.total_work_hour
  from get_total_hour_employee_monthly em
  group by em.year, em.month, em.branch_id, em.total_work_hour
  order by 1,2,3 asc) gs
  group by gs.year, gs.month, gs.branch_id
  order by 1,2,3 asc
)

, get_salary_hourly as (
  select
    bm.year
    , bm.month
    , bm.branch_id
    , format("%.2f", (bm.total_salary_branch / bm.total_work_hour_branch)) as salary_per_hour
  from get_total_salary_per_branch_monthly bm
  group by bm.year, bm.month, bm.branch_id, bm.total_salary_branch, bm.total_work_hour_branch
  order by 1,2,3
)

select year, month, branch_id, salary_per_hour from get_salary_hourly
