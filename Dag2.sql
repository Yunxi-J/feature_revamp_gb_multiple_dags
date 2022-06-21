-- ovd
DROP TABLE IF EXISTS sandbox_analytics_us.instal_ovd_d360;
CREATE  TABLE sandbox_analytics_us.instal_ovd_d360 as (
    with ovd as (
        select *
        from green.raw_p_e_pymt_schd_became_overdue
        where par_region in ('GB')
          and par_process_date >= DATE(SYSDATE) - 360
    ),
         expt as (
             select *
                  , case
                        when upper(note) like '%LOSTSTOLENCARD%' or upper(note) like '%LOST/STOLEN%'
                            THEN 'Lost_Stolen_Card'
                        when upper(note) like '%CHARGEBACKS%' THEN 'Chargebacks'
                        when upper(note) like '%ORDERDISPUTES%' THEN 'Order_Disputes'
                        when upper(note) like '%HARDSHIP%' THEN 'Hardship'
                        when upper(note) like '%COMPLAINT%' THEN 'Complaint'
                        when upper(note) like '%TECHISSUE%' THEN 'Tech Issue'
                        when upper(note) like '%CAPITALONE%' THEN 'Capital One'
                        when upper(note) like '%DISASTER%' then 'Disaster'
                        when upper(note) like '%GOODWILL%' or upper(note) like '%GOOD WILL%' then 'Goodwill'
                        when upper(note) like '%INCIDENT REMEDIATION%' THEN regexp_substr(note, '#[^/ ]*')
                        else 'Others' end                                                           as category
                  , ROW_NUMBER()
                    over (partition by key_payment_schedule_id order by event_info_event_time desc) as _dedup
             from green.raw_p_e_payment_schdl_override
             where par_region in ('GB')
               and par_process_date >= DATE(SYSDATE) - 360
         )

    select distinct ovd.key_payment_schedule_id
                  , ovd.order_transaction_id
                  , i.consumer_uuid
                  , i.country_code
                  , ovd.event_info_event_time                                                         as ovd_ts
                  , COALESCE(expt.event_info_event_time, 0)                                           as expt_ts
                  , i.upfront_instalment
                  , i.current_due_date
                  , i.paid_cnt
                  , i.latest_instal_paid_date
                  , i.latest_instal_waived_date
                  , COALESCE(case when expt.override_overdue_status = true then 1 else 0 end, 0)      as ovd_expt
                  , COALESCE(case when expt.override_overdue_status is not null then 1 else 0 end, 0) as expt_record
                  , expt.category
                  , COALESCE(i.full_waived, 0) as full_waived
    from ovd
             inner join sandbox_analytics_us.instal_events_agg_gb i
                        on ovd.key_payment_schedule_id = i.instalment_id
             left join expt
                       on ovd.key_payment_schedule_id = expt.key_payment_schedule_id
                           and ovd.order_transaction_id = expt.key_order_transaction_id
                           and expt._dedup = 1
                           and DATE(green.fn_in_local_tz(i.country_code, TIMESTAMP 'epoch' +
                                                                         expt.event_info_event_time / 1000 *
                                                                         interval '1 second')) between DATE(green.fn_in_local_tz(i.country_code, SYSDATE)) - 360 and DATE(green.fn_in_local_tz(i.country_code, SYSDATE)) - 1
    where DATE(green.fn_in_local_tz(i.country_code, TIMESTAMP 'epoch' + ovd.event_info_event_time / 1000 *
                                                                        interval '1 second')) between DATE(green.fn_in_local_tz(i.country_code, SYSDATE)) - 360 and DATE(green.fn_in_local_tz(i.country_code, SYSDATE))
);

select * from sandbox_analytics_us.instal_ovd_d360  limit 100;


CREATE TABLE sandbox_analytics_us.cust_ovd_d360 distkey(consumer_uuid) as (
    select consumer_uuid
         , COALESCE(min(current_due_date), '1969-12-31') as c_earliest_late_pymt_due_date_360d
         , COALESCE(max(current_due_date), '1969-12-31') as c_latest_late_pymt_due_date_360d
    from sandbox_analytics_us.instal_ovd_d360
    where ovd_expt = 0
    group by 1
);
select * from sandbox_analytics_us.cust_ovd_d360 limit 100;



-- due_date
DROP TABLE IF EXISTS sandbox_analytics_us.cust_instal_d360;
CREATE TABLE sandbox_analytics_us.cust_instal_d360 distkey(consumer_uuid) AS (
    with instal_tmp as (
        SELECT instal.consumer_uuid
             , instal.instalment_id
             , case
                   when instal.full_waived = 1 then '1969-12-31'
                   when instal.payment_cnt > 0 or ovd.key_payment_schedule_id is not null or
                        instal.current_due_date >= DATE(green.fn_in_local_tz(instal.country_code, SYSDATE))
                       then instal.current_due_date
                   else '1969-12-31' end as instal_due_date
             , case
                   when instal.original_due_date < instal.current_due_date then instal.original_due_date
                   else '1969-12-31' end as postpone_due_date
        FROM sandbox_analytics_us.instal_events_agg_us instal
                 LEFT JOIN sandbox_analytics_us.instal_ovd_d360 ovd
                           ON instal.instalment_id = ovd.key_payment_schedule_id
        WHERE DATE(instal.current_due_date) >= DATE(green.fn_in_local_tz(instal.country_code, SYSDATE)) - 360
    )
    select consumer_uuid
         , max(instal_due_date)   as c_latest_instal_due_date_360d
         , max(postpone_due_date) as c_latest_instal_postpone_due_date_360d
    from instal_tmp
    group by 1
);
select * from sandbox_analytics_us.cust_instal_d360 limit 100;


-- due_date
DROP TABLE IF EXISTS sandbox_analytics_us.cust_instal_d360;
CREATE TABLE sandbox_analytics_us.cust_instal_d360 distkey(consumer_uuid) AS (
    with instal_tmp as (
        SELECT instal.consumer_uuid
             , instal.instalment_id
             , case
                   when instal.full_waived = 1 then '1969-12-31'
                   when instal.payment_cnt > 0 or ovd.key_payment_schedule_id is not null or
                        instal.current_due_date >= DATE(green.fn_in_local_tz(instal.country_code, SYSDATE))
                       then instal.current_due_date
                   else '1969-12-31' end as instal_due_date
             , case
                   when instal.original_due_date < instal.current_due_date then instal.original_due_date
                   else '1969-12-31' end as postpone_due_date
        FROM sandbox_analytics_us.instal_events_agg_us instal
                 LEFT JOIN sandbox_analytics_us.instal_ovd_d360 ovd
                           ON instal.instalment_id = ovd.key_payment_schedule_id
        WHERE DATE(instal.current_due_date) >= DATE(green.fn_in_local_tz(instal.country_code, SYSDATE)) - 360
    )
    select consumer_uuid
         , max(instal_due_date)   as c_latest_instal_due_date_360d
         , max(postpone_due_date) as c_latest_instal_postpone_due_date_360d
    from instal_tmp
    group by 1
);


DROP TABLE IF EXISTS sandbox_analytics_us.instal_ontime_d180;
CREATE TABLE sandbox_analytics_us.instal_ontime_d180 DISTKEY(instalment_id) AS (
    SELECT instal.*
    FROM sandbox_analytics_us.instal_events_agg_us instal
             LEFT JOIN sandbox_analytics_us.instal_ovd_d360 ovd
                       ON instal.instalment_id = ovd.key_payment_schedule_id
    WHERE ovd.key_payment_schedule_id is null
      AND instal.paid_cnt >= 1
      AND DATE(instal.current_due_date) >= DATE(green.fn_in_local_tz(instal.country_code, SYSDATE)) - 180
      AND instal.upfront_instalment = 0
);

select * from sandbox_analytics_us.instal_ontime_d180 limit 100;

-- DROP TABLE IF EXISTS sandbox_analytics_us.instal_events_agg_us;

DROP TABLE IF EXISTS sandbox_analytics_us.cust_ontime_d180;
CREATE TABLE sandbox_analytics_us.cust_ontime_d180 DISTKEY(consumer_uuid) AS (
    select consumer_uuid
         , count(1) as c_ontime_pymt_cnt_180d
    from sandbox_analytics_us.instal_ontime_d180
    group by 1
);

select * from sandbox_analytics_us.cust_ontime_d180 limit 100;



DROP TABLE IF EXISTS sandbox_analytics_us.cust_ovd_d180;
CREATE
    TABLE sandbox_analytics_us.cust_ovd_d180 as (
    with ovd_d180 as (
        select ovd.*
        from sandbox_analytics_us.instal_ovd_d360 ovd
        where DATE(ovd.current_due_date) >= DATE(green.fn_in_local_tz(ovd.country_code, SYSDATE)) - 180
          and ovd_expt = 0
    ),
         tmp as (
             select consumer_uuid
                  , count(1)              as c_late_pymt_cnt_d180
                  , max(current_due_date) as last_due_event_date
                  , max(date_diff('day', current_due_date, (case
                                                                when paid_cnt >= 1 then latest_instal_paid_date
                                                                when full_waived = 1 then latest_instal_waived_date
                                                                else date(green.fn_in_local_tz(country_code, SYSDATE))
                 end)))                       as c_day_late_pymt_max_cnt_180d
                  , sum(date_diff('day', current_due_date, (case
                                                                when paid_cnt >= 1 then latest_instal_paid_date
                                                                when full_waived = 1 then latest_instal_waived_date
                                                                else date(green.fn_in_local_tz(country_code, SYSDATE))
                 end)))                   as c_day_late_pymt_tot_cnt_180d
             from ovd_d180
             where date_diff('day', current_due_date, (case
                                                           when paid_cnt >= 1 then latest_instal_paid_date
                                                           when full_waived = 1 then latest_instal_waived_date
                                                           else date(green.fn_in_local_tz(country_code, SYSDATE))
                 end)) >= 0
             group by 1
         )
    select tmp.consumer_uuid
         , max(tmp.c_late_pymt_cnt_d180) as c_late_pymt_cnt_180d
         , max(tmp.c_day_late_pymt_max_cnt_180d) as c_day_late_pymt_max_cnt_180d
         , max(tmp.c_day_late_pymt_tot_cnt_180d) as c_day_late_pymt_tot_cnt_180d
         , sum(case
                   when (ot.consumer_uuid is not null) and
                        (ot.latest_instal_paid_date > tmp.last_due_event_date) and
                        (ot.upfront_instalment = 0) then 1
                   else 0 end) as c_ontime_pymt_cnt_llate_180d
    from tmp
             left join sandbox_analytics_us.instal_ontime_d180 ot
                       on tmp.consumer_uuid = ot.consumer_uuid
    group by 1
);

select * from sandbox_analytics_us.cust_ovd_d180 limit 100;


-- late_expt
DROP TABLE IF EXISTS sandbox_analytics_us.cust_late_expt_d360;
CREATE TABLE sandbox_analytics_us.cust_late_expt_d360 distkey(consumer_uuid) as (
    select consumer_uuid
         , max(date(green.fn_in_local_tz(country_code, TIMESTAMP 'epoch' + expt_ts / 1000 *
                                                                           interval '1 second'))) as c_latest_late_expt_date_360d
         , max(date(green.fn_in_local_tz(country_code, TIMESTAMP 'epoch' + (case when category = 'Goodwill' then expt_ts else 0 end) / 1000 *
                                                                           interval '1 second'))) as c_latest_late_expt_goodwill_date_360d
    from sandbox_analytics_us.instal_ovd_d360
    where ovd_expt = 1
    group by 1
);

select * from sandbox_analytics_us.cust_late_expt_d360 limit 100;


DROP TABLE IF EXISTS sandbox_analytics_us.cust_ovd_limit_d360;
CREATE TABLE sandbox_analytics_us.cust_ovd_limit_d360 as (
    with ovd_tmp as (
        select ovd.*
        from sandbox_analytics_us.line_change_window line
                 left join sandbox_analytics_us.instal_ovd_d360 ovd
                           on line.consumer_consumer_uuid = ovd.consumer_uuid
                               and date(green.fn_in_local_tz(ovd.country_code, TIMESTAMP 'epoch' +
                                                                               ovd_ts / 1000 * interval '1 second')) >=
                                   date(line.line_change_ts_360d)
                               and ovd.ovd_expt = 0
    )
    select consumer_uuid
         , max(date_diff('day', current_due_date, (case
                                                       when paid_cnt >= 1 then latest_instal_paid_date
                                                       when full_waived = 1 then latest_instal_waived_date
                                                       else date(green.fn_in_local_tz(country_code, SYSDATE))
        end))) as c_day_late_pymt_max_cnt_after_limit_change_360d
    from ovd_tmp
    where date_diff('day', current_due_date, (case
                                                  when paid_cnt >= 1 then latest_instal_paid_date
                                                  when full_waived = 1 then latest_instal_waived_date
                                                  else date(green.fn_in_local_tz(country_code, SYSDATE))
        end)) >= 0
    group by 1
);

select * from sandbox_analytics_us.cust_ovd_limit_d360 limit 100;




drop table if exists sandbox_analytics_us.util_d180;
create table sandbox_analytics_us.util_d180 distkey(order_id) as (
    select line.consumer_consumer_uuid
         , COALESCE(otb.order_id, '')                                                                           as order_id
         , COALESCE(otb.key_id, -1)                                                                             as pos_pre_approval_id
         , COALESCE(otb.otb_amount, 0)                                                                          as otb_amount
         , COALESCE(otb.account_limit_amount, 0)                                                                as limit_amount
         , COALESCE(otb.osb_amount, 0)                                                                          as osb_amount
         , line.country_code
         , otb.key_event_info_event_time
         , green.fn_in_local_tz(line.country_code, TIMESTAMP 'epoch' + otb.key_event_info_event_time / 1000 *
                                                                       interval '1 second')                     as otb_event_ts
    from sandbox_analytics_us.line_change_window line
             left join sandbox_analytics_us.otb_tmp otb
                       on line.consumer_consumer_uuid = otb.consumer_uuid
                           and date(green.fn_in_local_tz(line.country_code, TIMESTAMP 'epoch' +
                                                                            otb.key_event_info_event_time / 1000 *
                                                                            interval '1 second')) >=
                               date(line.line_change_ts_180d)
);

select * from sandbox_analytics_us.util_d180 limit 100;

-- c_max_utilization_180d
drop table if exists sandbox_analytics_us.cust_util_d180;
create table sandbox_analytics_us.cust_util_d180 distkey (consumer_uuid) as (
    with online as (
        select base.consumer_consumer_uuid
             , util.order_id
             , util.otb_amount
             , util.limit_amount
             , util.osb_amount
             , base.order_amt
             , util.osb_amount + base.order_amt as utilization
        from sandbox_analytics_us.order_d360 base
                 left join sandbox_analytics_us.util_d180 util
                           on base.consumer_consumer_uuid = util.consumer_consumer_uuid
                               and base.key_token = util.order_id
        where DATE(base.order_datetime) between DATE(green.fn_in_local_tz(base.country_code, SYSDATE)) - 180 and DATE(green.fn_in_local_tz(base.country_code, SYSDATE)) - 1

    ),
         instore as (
             select base.consumer_consumer_uuid
                  , util.order_id
                  , util.otb_amount
                  , util.limit_amount
                  , util.osb_amount
                  , base.order_amt
                  , util.osb_amount + base.order_amt as utilization
             from sandbox_analytics_us.order_d360 base
                      left join sandbox_analytics_us.util_d180 util
                                on base.consumer_consumer_uuid = util.consumer_consumer_uuid
                                    and base.pos_pre_approval_id = util.pos_pre_approval_id
             where base.pos_pre_approval_id <> -1
               and DATE(base.order_datetime) between DATE(green.fn_in_local_tz(base.country_code, SYSDATE)) - 180 and DATE(green.fn_in_local_tz(base.country_code, SYSDATE)) - 1

         ),
         limit_check as (
             select util.consumer_consumer_uuid
                  , util.order_id
                  , util.otb_amount
                  , util.limit_amount
                  , util.osb_amount
                  , 0 as order_amt
                  , util.osb_amount as utilization
             from sandbox_analytics_us.util_d180 util
             where order_id = ''
         ),
         tbl as (
             select *
             from online
             union all
             select *
             from instore
             union all
             select *
             from limit_check
         )
    select consumer_consumer_uuid        as consumer_uuid
         , COALESCE(max(utilization), 0) as c_max_util_amt_after_limit_change_180d
    from tbl
    group by 1
);
select * from sandbox_analytics_us.cust_util_d180 limit 100;

-- FINAL OUTPUT!!!
-- drop table if exists sandbox_analytics_us.credit_model_revamp_cust_master_us;
-- create table sandbox_analytics_us.credit_model_revamp_cust_master_us distkey (consumer_uuid) as (
delete from curated_feature_science_green.credit_model_revamp_cust_master_gb;
insert into curated_feature_science_green.credit_model_revamp_cust_master_gb

select base.par_region
     , DATE(SYSDATE)                                                             as par_process_date
     , base.consumer_id
     , base.consumer_uuid
     , base.country_code
     , base.c_tenure
     , COALESCE(cpm.c_best_payment_type, 'DEBIT')                                as c_best_payment_type
     , COALESCE(co_d180.c_order_cnt_180d, 0)                                     as c_order_cnt_180d_v3
     , COALESCE(co_d180.c_topaz_decl_insufficent_funds_cnt_180d, 0)              as c_topaz_decl_insufficent_funds_cnt_180d_v3
     , COALESCE(co_d180.c_order_attempt_cnt_180d, 0)                             as c_order_attempt_cnt_180d_v3
     , COALESCE(cot_d180.c_ontime_pymt_cnt_180d, 0)                              as c_ontime_pymt_cnt_180d_v3
     , COALESCE(covd_d180.c_ontime_pymt_cnt_llate_180d, 0)                       as c_ontime_pymt_cnt_llate_180d_v3
     , COALESCE(covd_d180.c_day_late_pymt_max_cnt_180d, 0)                       as c_day_late_pymt_max_cnt_180d_v3
     , COALESCE(covd_d180.c_day_late_pymt_tot_cnt_180d, 0)                       as c_day_late_pymt_tot_cnt_180d_v3
     , COALESCE(climit.c_latest_approved_credit_limit, 0)                        as c_latest_approved_credit_limit_v3
     , COALESCE(covd_d360.c_earliest_late_pymt_due_date_360d,
                '1969-12-31')                                                    as c_earliest_late_pymt_due_date_360d_v3
     , COALESCE(covd_d360.c_latest_late_pymt_due_date_360d, '1969-12-31')        as c_latest_late_pymt_due_date_360d_v3
     , COALESCE(clf_d360.c_late_fee_paid_amount_360d, 0)                         as c_late_fee_paid_amount_360d_v3
     , COALESCE(clf_d360.c_late_fee_paid_amount_90d, 0)                          as c_late_fee_paid_amount_90d_v3
     , COALESCE(clf_d360.c_late_fee_paid_cnt_360d, 0)                            as c_late_fee_paid_cnt_360d_v3
     , COALESCE(clf_d360.c_late_fee_paid_cnt_90d, 0)                             as c_late_fee_paid_cnt_90d_v3
     , COALESCE(clf_d360.c_first_late_fee_paid_datetime_360d,
                '1969-12-31 11:59:59')                                           as c_first_late_fee_paid_datetime_360d_v3
     , COALESCE(clf_d360.c_first_late_fee_paid_datetime_90d,
                '1969-12-31 11:59:59')                                           as c_first_late_fee_paid_datetime_90d_v3
     , COALESCE(cutil_d180.c_max_util_amt_after_limit_change_180d, 0)            as c_max_util_amt_after_limit_change_180d_v3
     , COALESCE(covdlim_d360.c_day_late_pymt_max_cnt_after_limit_change_360d,
                0)                                                               as c_day_late_pymt_max_cnt_after_limit_change_360d_v3
     , COALESCE(ci_d360.c_latest_instal_due_date_360d, '1969-12-31')             as c_latest_instal_due_date_360d_v3
     , COALESCE(ci_d360.c_latest_instal_postpone_due_date_360d,
                '1969-12-31')                                                    as c_latest_instal_postpone_due_date_360d_v3


     , COALESCE(cexpt_d360.c_latest_late_expt_date_360d, '1969-12-31')           as c_latest_late_expt_date_360d_v3
     , COALESCE(cexpt_d360.c_latest_late_expt_goodwill_date_360d,
                '1969-12-31')                                                    as c_latest_late_expt_goodwill_date_360d_v3
     , COALESCE(cao_d180.c_order_amt_180d, 0)                                    as c_order_amt_180d_v3
     , COALESCE(cao_d180.c_order_amt_percentile_180d, 0)                         as c_order_amt_percentile_180d_v3
     , COALESCE(cmlimit.c_prior_credit_limit_2d, 0)                              as c_prior_credit_limit_2d_v3
     , COALESCE(cmlimit.c_prior_credit_limit_3d, 0)                              as c_prior_credit_limit_3d_v3
     , COALESCE(cmlimit.c_prior_credit_limit_10d, 0)                             as c_prior_credit_limit_10d_v3
     , COALESCE(cmlimit.c_prior_credit_limit_30d, 0)                             as c_prior_credit_limit_30d_v3
     , COALESCE(cmlimit.c_prior_credit_limit_90d, 0)                             as c_prior_credit_limit_90d_v3
     , COALESCE(cmlimit.c_prior_credit_limit_180d, 0)                            as c_prior_credit_limit_180d_v3
     , COALESCE(clf_d360.c_late_fee_waived_amount_360d, 0)                       as c_late_fee_waived_amount_360d_v3
     , COALESCE(clf_d360.c_late_fee_waived_amount_90d, 0)                        as c_late_fee_waived_amount_90d_v3
     , COALESCE(clf_d360.c_late_fee_waived_cnt_360d, 0)                          as c_late_fee_waived_cnt_360d_v3
     , COALESCE(clf_d360.c_late_fee_waived_cnt_90d, 0)                           as c_late_fee_waived_cnt_90d_v3
     , COALESCE(clf_d360.c_late_fee_refunded_amount_360d, 0)                     as c_late_fee_refunded_amount_360d_v3
     , COALESCE(clf_d360.c_late_fee_refunded_amount_90d, 0)                      as c_late_fee_refunded_amount_90d_v3
     , COALESCE(clf_d360.c_late_fee_refunded_cnt_360d, 0)                        as c_late_fee_refunded_cnt_360d_v3
     , COALESCE(clf_d360.c_late_fee_refunded_cnt_90d, 0)                         as c_late_fee_refunded_cnt_90d_v3
from credit_model_revamp_cust_base_gb base
         left join cust_pymt_method cpm on base.consumer_uuid = cpm.consumer_uuid
         left join cust_order_d180 co_d180 on base.consumer_uuid = co_d180.consumer_uuid
         left join cust_approved_limit climit on base.consumer_uuid = climit.consumer_uuid
         left join cust_ontime_d180 cot_d180 on base.consumer_uuid = cot_d180.consumer_uuid
         left join cust_latefee_d360 clf_d360 on base.consumer_uuid = clf_d360.consumer_uuid
         left join cust_ovd_d180 covd_d180 on base.consumer_uuid = covd_d180.consumer_uuid
         left join cust_ovd_d360 covd_d360 on base.consumer_uuid = covd_d360.consumer_uuid
         left join cust_instal_d360 ci_d360 on base.consumer_uuid = ci_d360.consumer_uuid
         left join cust_late_expt_d360 cexpt_d360 on base.consumer_uuid = cexpt_d360.consumer_uuid
         left join cust_util_d180 cutil_d180 on base.consumer_uuid = cutil_d180.consumer_uuid
         left join cust_ovd_limit_d360 covdlim_d360 on base.consumer_uuid = covdlim_d360.consumer_uuid
         left join cust_approved_order_d180 cao_d180 on base.consumer_uuid = cao_d180.consumer_uuid
         left join cust_account_limit cmlimit on base.consumer_uuid = cmlimit.consumer_uuid
;
-- );




