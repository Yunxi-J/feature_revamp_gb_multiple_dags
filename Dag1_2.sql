--order base
drop table if exists sandbox_analytics_us.order_d360;
create table sandbox_analytics_us.order_d360 distkey(order_transaction_id) as (
    select order_transaction_id
         , key_token
         , pos_pre_approval_id
         , consumer_consumer_uuid
         , consumer_total_amount_amount::decimal(10, 2)                                as order_amt
         , green.fn_in_local_tz(country_code, TIMESTAMP 'epoch' + key_event_info_event_time / 1000 *
                                                                  interval '1 second') as order_datetime
         , country_code
    from green.raw_c_e_order
    where status IN ('APPROVED')
      and par_region IN ('GB')
      and par_process_date >= DATE(SYSDATE) - 360
      and DATE(order_datetime) between DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 360 and DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 1
);

select * from sandbox_analytics_us.order_d360 limit 100;


drop table if exists sandbox_analytics_us.line_change;
create table sandbox_analytics_us.line_change distkey(consumer_uuid) as (
    with dedup as (
        select *
             , COALESCE(
                (lead(old_value, 1)
                 over (partition by consumer_uuid order by timestamp desc))::decimal(10, 2)
            , 0) as last_old_value
        from red.raw_c_e_rulesenginekarma_decision_value_change
        where par_process_date >= DATE(SYSDATE) - 360
          and par_region in ('GB')
    )
    select consumer_uuid
         , max(timestamp) as ts
    from dedup
    where last_old_value::decimal(10, 2) <> old_value::decimal(10, 2)
      and DATE(green.fn_in_local_tz(country_code, TIMESTAMP 'epoch' + timestamp / 1000 *
                                                                      interval '1 second')) between DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 360 and DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 1
    group by 1
);

select * from sandbox_analytics_us.line_change limit 100;

insert into sandbox_analytics_us.line_change
select entity_id as consumer_uuid
     , GREATEST(c_latest_limit_increase_ts, c_latest_limit_decrease_ts) as ts
from curated_feature_science_green.limit_change_backfill_gb
where DATE(green.fn_in_local_tz(country_code, TIMESTAMP 'epoch' + ts / 1000 *
                                                                  interval '1 second')) between DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 360 and DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 1
;

drop table if exists sandbox_analytics_us.line_change_window;
create table sandbox_analytics_us.line_change_window distkey (consumer_consumer_uuid) as (
    with dedup as (
        select consumer_uuid
             , max(ts) as max_event_ts
        from sandbox_analytics_us.line_change
        group by 1
    ),
         tmp as (
             select base.consumer_consumer_uuid
                  , base.country_code
                  , dedup.max_event_ts
                  , green.fn_in_local_tz(base.country_code, TIMESTAMP 'epoch' +
                                                            dedup.max_event_ts / 1000 * interval '1 second') as event_ts
             from sandbox_analytics_us.order_d360 base
                      left join dedup
                                on base.consumer_consumer_uuid = dedup.consumer_uuid
         )
    select consumer_consumer_uuid
         , country_code
         , max(COALESCE(event_ts, green.fn_in_local_tz(country_code, SYSDATE) - 180)) as line_change_ts_180d
         , max(COALESCE(event_ts, green.fn_in_local_tz(country_code, SYSDATE) - 360)) as line_change_ts_360d
    from tmp
    group by 1, 2
);
select * from sandbox_analytics_us.line_change_window limit 100;
