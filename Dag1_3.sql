DROP TABLE IF EXISTS sandbox_analytics_us.de_limit;
CREATE TABLE sandbox_analytics_us.de_limit AS (
    with de_old as (
        select consumer_uuid
             , order_token                                                                 as order_id
             , pos_external_id
             , input_checkpointtype                                                        as checkpoint_type
             , account_limit_amount::decimal(10, 2)                                        as account_limit_amount
             , input_otbamount::decimal(10, 2)                                             as otb_amount
             , input_amountowing::decimal(10, 2)                                           as osb_amount
             , cast(DATEDIFF(ms, '1970-01-01 00:00:00.000', created_datetime) as bigint)   as event_info_event_time
             , created_datetime as limit_local_datetime
             , row_number() over (partition by order_token order by created_datetime desc) as row_num
        from red.raw_c_f_decision_vars
        where par_region IN ('GB')
          and par_created_date < '2021-06-01'
          and input_checkpointtype IN ('ONLINE_CHECKOUT_START', 'BARCODE_GENERATION')
          and approved = '1'
    ),
         de_new as (
             select consumer_uuid
                  , order_id
                  , pos_external_id
                  , check_point_type                                                              as checkpoint_type
                  , account_limit_amount::decimal(10, 2)                                          as account_limit_amount
                  , available_barcode_amount_amount::decimal(10, 2)                               as otb_amount
                  , nullif(json_extract_path_text(input_variables, 'amountOwing', 'value', TRUE),
                           '')::decimal(10, 2)                                                             as osb_amount
                  , event_info_event_time
                  , null as limit_local_datetime
                  , row_number() over (partition by order_id order by event_info_event_time desc) as row_num
             from red.raw_c_e_de_rules_engine_result
             where par_region IN ('GB')
               and par_process_date >= '2021-06-01'
               and check_point_type IN ('ONLINE_CHECKOUT_START', 'BARCODE_GENERATION')
               and approved = 'true'
         )
    select *
    from de_old
    where row_num = 1
    union all
    select *
    from de_new
    where row_num = 1
);

select * from sandbox_analytics_us.de_limit limit 100;


DROP TABLE IF EXISTS sandbox_analytics_us.cust_approved_limit;
CREATE TABLE sandbox_analytics_us.cust_approved_limit distkey(consumer_uuid) AS (
    with latest_order as (
        select consumer_consumer_uuid
             , max(country_code)              as country_code
             , max(key_event_info_event_time) as latest_order_datetime
        from green.raw_c_e_order
        where par_region IN ('GB')
          and par_process_date <= DATE(SYSDATE)
          and status IN ('APPROVED')
        group by 1
    )
    select *
    from (
             select base.consumer_consumer_uuid                                              as consumer_uuid
                  , de.account_limit_amount                                                  as c_latest_approved_credit_limit
                  , case
                        when limit_local_datetime is null then date(green.fn_in_local_tz(base.country_code,
                                                                                         TIMESTAMP 'epoch' +
                                                                                         de.event_info_event_time /
                                                                                         1000 *
                                                                                         interval '1 second'))
                        else date(limit_local_datetime) end                                  as event_date
                  , row_number()
                    over (partition by base.consumer_consumer_uuid order by event_date desc) as _dedup
             from latest_order base
                      left join sandbox_analytics_us.de_limit de
                                on base.consumer_consumer_uuid = de.consumer_uuid
                                    and date(green.fn_in_local_tz(country_code,
                                                                  TIMESTAMP 'epoch' +
                                                                  base.latest_order_datetime / 1000 *
                                                                  interval '1 second')) >= case
                                                                                               when limit_local_datetime is null
                                                                                                   then date(green.fn_in_local_tz(
                                                                                                       base.country_code,
                                                                                                       TIMESTAMP 'epoch' +
                                                                                                       de.event_info_event_time /
                                                                                                       1000 *
                                                                                                       interval '1 second'))
                                                                                               else date(limit_local_datetime) end
         )
    where _dedup = 1
);

select * from sandbox_analytics_us.cust_approved_limit limit 100;


-- append outstanding_balance
drop table if exists sandbox_analytics_us.order_attempt;
create table sandbox_analytics_us.order_attempt distkey(consumer_uuid) as (
    with tmp_pa as (
        select *, row_number() over (partition by external_id order by event_info_event_time desc) as row_num
        from green.raw_c_e_pre_approval pa
        where pa.par_process_date >= DATE(SYSDATE) - 180
          and pa.par_region in ('GB')
    )
    select de_limit.consumer_uuid
         , de_limit.order_id
         , tmp_pa.key_id
         , de_limit.event_info_event_time as key_event_info_event_time
         , de_limit.otb_amount
         , de_limit.account_limit_amount
         , de_limit.osb_amount
    from sandbox_analytics_us.de_limit
             left join tmp_pa
                       on de_limit.pos_external_id = tmp_pa.external_id
                           and tmp_pa.row_num = 1
);

select * from sandbox_analytics_us.order_attempt limit 100;


drop table if exists sandbox_analytics_us.limit_check;
create table sandbox_analytics_us.limit_check distkey(consumer_uuid) as (
    select consumer_uuid
         , null as order_id
         , -1 as key_id
         , cast(DATEDIFF(ms, '1970-01-01 00:00:00.000', event_info_event_time) as bigint) as key_event_info_event_time
         , otb_amount_amount::decimal(10, 2)    as otb_amount
         , account_limit_amount::decimal(10, 2) as account_limit_amount
         , (account_limit_amount - otb_amount_amount)::decimal(10, 2) as osb_amount
    from red.raw_c_e_de_credit_limit_result
    where par_process_date >= DATE(SYSDATE) - 180
      and par_region in ('GB')
);

select * from sandbox_analytics_us.limit_check limit 100;

drop table if exists sandbox_analytics_us.otb_tmp;
create table sandbox_analytics_us.otb_tmp distkey(consumer_uuid) as (
    select *
    from sandbox_analytics_us.order_attempt
    union all
    select *
    from sandbox_analytics_us.limit_check
);


DROP TABLE IF EXISTS sandbox_analytics_us.cust_approved_limit;
CREATE TABLE sandbox_analytics_us.cust_approved_limit distkey(consumer_uuid) AS (
    with latest_order as (
        select consumer_consumer_uuid
             , max(country_code)              as country_code
             , max(key_event_info_event_time) as latest_order_datetime
        from green.raw_c_e_order
        where par_region IN ('GB')
          and par_process_date <= DATE(SYSDATE)
          and status IN ('APPROVED')
        group by 1
    )
    select *
    from (
             select base.consumer_consumer_uuid                                              as consumer_uuid
                  , de.account_limit_amount                                                  as c_latest_approved_credit_limit
                  , case
                        when limit_local_datetime is null then date(green.fn_in_local_tz(base.country_code,
                                                                                         TIMESTAMP 'epoch' +
                                                                                         de.event_info_event_time /
                                                                                         1000 *
                                                                                         interval '1 second'))
                        else date(limit_local_datetime) end                                  as event_date
                  , row_number()
                    over (partition by base.consumer_consumer_uuid order by event_date desc) as _dedup
             from latest_order base
                      left join sandbox_analytics_us.de_limit de
                                on base.consumer_consumer_uuid = de.consumer_uuid
                                    and date(green.fn_in_local_tz(country_code,
                                                                  TIMESTAMP 'epoch' +
                                                                  base.latest_order_datetime / 1000 *
                                                                  interval '1 second')) >= case
                                                                                               when limit_local_datetime is null
                                                                                                   then date(green.fn_in_local_tz(
                                                                                                       base.country_code,
                                                                                                       TIMESTAMP 'epoch' +
                                                                                                       de.event_info_event_time /
                                                                                                       1000 *
                                                                                                       interval '1 second'))
                                                                                               else date(limit_local_datetime) end
         )
    where _dedup = 1
);

select * from sandbox_analytics_us.cust_approved_limit limit 100;
