features:
--c_first_late_fee_paid_datetime_360d
--c_first_late_fee_paid_datetime_90d
--c_late_fee_paid_amount_360d
--c_late_fee_paid_amount_90d
--c_late_fee_paid_cnt_360d
--c_late_fee_paid_cnt_90d


-- late fee
DROP TABLE IF EXISTS sandbox_analytics_us.latefee_invoice;
CREATE TABLE sandbox_analytics_us.latefee_invoice distkey(late_fee_id) AS (
    SELECT DISTINCT orders.consumer_consumer_uuid AS consumer_uuid
                  , orders.country_code
                  , orders.order_transaction_id AS order_id
                  , lf.payment_schedule_id AS instalment_id
                  , lf.key_late_fee_id AS late_fee_id
                  , lf.event_info_event_time AS event_time
                  , lf.amount_amount::DECIMAL(20,2) AS amount_invoiced
                  , lf.par_region
    FROM green.raw_p_e_late_fee_applied lf
             INNER JOIN green.raw_p_e_payment_scheduled ps
                        ON lf.payment_schedule_id = ps.key_payment_schedule_id
                            AND lf.par_region = ps.par_region
             INNER JOIN green.raw_c_e_order orders
                        ON orders.order_transaction_id = ps.order_transaction_id
                            AND orders.par_region = ps.par_region
    WHERE orders.status IN ('APPROVED')
      AND lf.par_region in ('GB')
      AND ps.par_region IN ('GB')
      AND orders.par_region IN ('GB')
      AND lf.par_process_date >= DATE(SYSDATE) - 450
      AND ps.par_process_date >= DATE(SYSDATE) - 450
      AND orders.par_process_date >= DATE(SYSDATE) - 450
);

select * from sandbox_analytics_us.latefee_invoice limit 100;

DROP TABLE IF EXISTS sandbox_analytics_us.latefee_events;
CREATE TABLE sandbox_analytics_us.latefee_events AS (
    SELECT DISTINCT pp.consumer_uuid
                  , lf.country_code
                  , lf.instalment_id
                  , lf.order_id
                  , p.par_region
                  , p.key_late_fee_id               AS late_fee_id
                  , 'PAYMENT'::VARCHAR(16)          AS event_type
                  , p.payment_id
                  , p.event_time
                  , 0::DECIMAL(20, 2)               AS amount_invoiced
                  , p.amount_amount::DECIMAL(20, 2) AS amount_paid
                  , 0::DECIMAL(20, 2)               AS amount_waived
                  , 0::DECIMAL(20, 2)               AS amount_refunded
    FROM green.raw_p_e_pymt_prcsd_latefee_pymt p
             INNER JOIN (
        SELECT key_consumer_consumer_uuid                                                          as consumer_uuid
             , key_payment_id
             , par_region
             , payment_method_discount_name
             , event_info_event_time
             , ROW_NUMBER() over (partition by key_payment_id order by event_info_event_time desc) as _dedup
        FROM green.raw_p_e_payment_processed
        WHERE payment_status IN ('SUCCESSFUL')
          AND par_region IN ('GB')
          AND par_process_date >= DATE(SYSDATE) - 450
    ) pp
                        ON pp.key_payment_id = p.payment_id
                            AND pp._dedup = 1
             LEFT JOIN sandbox_analytics_us.latefee_invoice lf
                       ON lf.late_fee_id = p.key_late_fee_id
    WHERE p.par_region IN ('GB')
      AND p.par_process_date >= DATE(SYSDATE) - 450
);

select * from sandbox_analytics_us.latefee_events limit 100;


INSERT INTO sandbox_analytics_us.latefee_events
SELECT DISTINCT lf.consumer_uuid
              , lf.country_code
              , lf.instalment_id
              , lf.order_id
              , lf.par_region
              , lf.late_fee_id
              , 'INVOICED' AS event_type
              , NULL::BIGINT AS payment_id
              , lf.event_time
              , lf.amount_invoiced
              , 0::DECIMAL(20,2) AS amount_paid
              , 0::DECIMAL(20,2) AS amount_waived
              , 0::DECIMAL(20,2) AS amount_refunded
FROM sandbox_analytics_us.latefee_invoice lf
;


INSERT INTO sandbox_analytics_us.latefee_events
SELECT DISTINCT lf.consumer_uuid
              , lf.country_code
              , lf.instalment_id
              , lf.order_id
              , lf.par_region
              , waive.key_late_fee_id AS late_fee_id
              ,'WAIVED' AS event_type
              , NULL::BIGINT AS payment_id
              , waive.event_info_event_time as event_time
              , 0::DECIMAL(20,2) AS amount_invoiced
              , 0::DECIMAL(20,2) AS amount_paid
              , waive.amount_amount::DECIMAL(20,2) AS amount_waived
              , 0::DECIMAL(20,2) AS amount_refunded
FROM green.raw_p_e_late_fee_waived waive
         LEFT JOIN sandbox_analytics_us.latefee_invoice lf
                   ON waive.key_late_fee_id = lf.late_fee_id
                       AND waive.payment_schedule_id = lf.instalment_id
                       AND waive.par_region = lf.par_region
WHERE waive.par_region IN ('GB')
  AND waive.par_process_date >= DATE(SYSDATE) - 450
;


INSERT INTO sandbox_analytics_us.latefee_events
SELECT DISTINCT lf.consumer_uuid
              , lf.country_code
              , lf.instalment_id
              , lf.order_id
              , lf.par_region
              , refund.key_late_fee_id AS late_fee_id
              ,'REFUNDED' AS event_type
              , NULL::BIGINT AS payment_id
              , refund.event_info_event_time as event_time
              , 0::DECIMAL(20,2) AS amount_invoiced
              , 0::DECIMAL(20,2) AS amount_paid
              , 0::DECIMAL(20,2) AS amount_waived
              , refund.amount_amount::DECIMAL(20,2) AS amount_refunded
FROM green.raw_p_e_late_fee_refunded refund
         LEFT JOIN sandbox_analytics_us.latefee_invoice lf
                   ON refund.key_late_fee_id = lf.late_fee_id
                       AND refund.par_region = lf.par_region
WHERE refund.par_region IN ('GB')
  AND refund.par_process_date >= DATE(SYSDATE) - 450
;

select * from sandbox_analytics_us.latefee_events limit 100;

DROP TABLE IF EXISTS sandbox_analytics_us.latefee_events_agg;
CREATE TABLE sandbox_analytics_us.latefee_events_agg distkey(late_fee_id) AS (
    SELECT lf.consumer_uuid
         , dc.country_code
         , lf.order_id
         , lf.instalment_id
         , lf.late_fee_id
         , SUM(CASE WHEN lf.event_type IN ('INVOICED') THEN lf.amount_invoiced ELSE 0 END) AS amount_invoiced
         , SUM(CASE WHEN lf.event_type IN ('PAYMENT') THEN lf.amount_paid ELSE 0 END)      AS amount_paid
         , SUM(CASE WHEN lf.event_type IN ('WAIVED') THEN lf.amount_waived ELSE 0 END)     AS amount_waived
         , SUM(CASE WHEN lf.event_type IN ('REFUNDED') THEN lf.amount_refunded ELSE 0 END) AS amount_refunded
         , MAX(CASE WHEN lf.event_type IN ('INVOICED') THEN lf.event_time END)             AS invoiced_time
         , MAX(CASE WHEN lf.event_type IN ('PAYMENT') THEN lf.event_time END)              AS paid_time
         , MAX(CASE WHEN lf.event_type IN ('WAIVED') THEN lf.event_time END)               AS waived_time
         , MAX(CASE WHEN lf.event_type IN ('REFUNDED') THEN lf.event_time END)             AS refunded_time
    FROM sandbox_analytics_us.latefee_events lf
             INNER JOIN green.d_consumer dc
                        ON lf.consumer_uuid = dc.uuid
    WHERE DATE(green.fn_in_local_tz(dc.country_code, TIMESTAMP 'epoch' + lf.event_time / 1000 *
                                                                         interval '1 second')) between DATE(green.fn_in_local_tz(dc.country_code, SYSDATE)) - 360 and DATE(green.fn_in_local_tz(dc.country_code, SYSDATE)) - 1
    GROUP BY 1, 2, 3, 4, 5
);

select * from sandbox_analytics_us.latefee_events_agg limit 100;

DROP TABLE IF EXISTS sandbox_analytics_us.cust_latefee_d360;
CREATE TABLE sandbox_analytics_us.cust_latefee_d360 distkey(consumer_uuid) AS (
    with tmp as (
        SELECT consumer_uuid
             , country_code
             , SUM(CASE
                       WHEN DATE(green.fn_in_local_tz(country_code, TIMESTAMP 'epoch' + paid_time / 1000 *
                                                                                        interval '1 second')) between DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 360 and DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 1
                           THEN amount_paid
                       ELSE 0 END)                                                                                    AS c_late_fee_paid_amount_360d
             , SUM(CASE
                       WHEN DATE(green.fn_in_local_tz(country_code, TIMESTAMP 'epoch' + paid_time / 1000 *
                                                                                        interval '1 second')) between DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 90 and DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 1
                           THEN amount_paid
                       ELSE 0 END)                                                                                    AS c_late_fee_paid_amount_90d
             , COUNT(DISTINCT CASE
                                  WHEN DATE(green.fn_in_local_tz(country_code, TIMESTAMP 'epoch' + paid_time / 1000 *
                                                                                                   interval '1 second')) between DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 360 and DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 1
                                      THEN late_fee_id END)                                                           AS c_late_fee_paid_cnt_360d
             , COUNT(DISTINCT CASE
                                  WHEN DATE(green.fn_in_local_tz(country_code, TIMESTAMP 'epoch' + paid_time / 1000 *
                                                                                                   interval '1 second')) between DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 90 and DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 1
                                      THEN late_fee_id END)                                                           AS c_late_fee_paid_cnt_90d
             , MIN(CASE
                       WHEN DATE(green.fn_in_local_tz(country_code, TIMESTAMP 'epoch' + paid_time / 1000 *
                                                                                        interval '1 second')) between DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 360 and DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 1
                           THEN green.fn_in_local_tz(country_code, TIMESTAMP 'epoch' + paid_time / 1000 *
                                                                                       interval '1 second') END)      AS c_first_late_fee_paid_datetime_360d
             , MIN(CASE
                       WHEN DATE(green.fn_in_local_tz(country_code, TIMESTAMP 'epoch' + paid_time / 1000 *
                                                                                        interval '1 second')) between DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 90 and DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 1
                           THEN green.fn_in_local_tz(country_code, TIMESTAMP 'epoch' + paid_time / 1000 *
                                                                                       interval '1 second') END)      AS c_first_late_fee_paid_datetime_90d
        from sandbox_analytics_us.latefee_events_agg
        group by 1, 2
    )
    select consumer_uuid
         , country_code
         , COALESCE(c_late_fee_paid_amount_360d, 0)                                               AS c_late_fee_paid_amount_360d
         , COALESCE(c_late_fee_paid_amount_90d, 0)                                                AS c_late_fee_paid_amount_90d
         , COALESCE(c_late_fee_paid_cnt_360d, 0)                                                  AS c_late_fee_paid_cnt_360d
         , COALESCE(c_late_fee_paid_cnt_90d, 0)                                                   AS c_late_fee_paid_cnt_90d
         , SUBSTRING(COALESCE(c_first_late_fee_paid_datetime_360d, '1969-12-31 11:59:59'), 0, 20) AS c_first_late_fee_paid_datetime_360d
         , SUBSTRING(COALESCE(c_first_late_fee_paid_datetime_90d, '1969-12-31 11:59:59'), 0, 20)  AS c_first_late_fee_paid_datetime_90d
    from tmp
);
select * from sandbox_analytics_us.cust_latefee_d360 limit 100;



features:
c_best_payment_type

Vega tables used:
raw_p_e_payment_processed

create table sandbox_analytics_us.cust_pymt_method as(
    with tmp as (
        select key_consumer_consumer_uuid as consumer_uuid
             , max(case
                       when payment_method_credit_card_card_type = 'CREDIT' then 1
                       else 0 end)        as c_payment_type
        from green.raw_p_e_payment_processed
        where par_region IN ('GB')
          and par_process_date <= DATE(SYSDATE)
        group by 1
    )
    select consumer_uuid
         , case
               when c_payment_type = 1 then 'CREDIT'
               else 'DEBIT' end as c_best_payment_type
    from tmp);


features:
c_best_payment_type

Vega tables used:
c_order_cnt_180d_v3
c_order_attempt_cnt_180d_v3
c_topaz_decl_insufficent_funds_cnt_180d_v3

create table sandbox_analytics_us.cust_order_d180 as
    (
        with tmp as (
            select *
            from green.raw_c_e_order
            where par_region IN ('GB')
              and par_process_date >= DATE(SYSDATE) - 180
        )
        select consumer_consumer_uuid                                                                                 as consumer_uuid
             , sum(case
                       when (key_token is not null) and (status in ('APPROVED')) then 1
                       else 0 end)                                                                                    as c_order_cnt_180d
             , sum(case
                       when (key_token is not null) and (status_reason in ('INSUFFICIENT_FUNDS')) then 1
                       else 0 end)                                                                                    as c_topaz_decl_insufficent_funds_cnt_180d
             , sum(case
                       when (key_token is not null) and (status_reason in
                                                         ('APPROVED', 'INSUFFICIENT_FUNDS', 'INVALID_PAYMENT_DETAILS',
                                                          'FRAUD_RULE_BREACH', 'DECLINED')) then 1
                       else 0 end)                                                                                    as c_order_attempt_cnt_180d
        from tmp
        where DATE(green.fn_in_local_tz(country_code, TIMESTAMP 'epoch' + key_event_info_event_time / 1000 *
                                                                          interval '1 second')) between DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 180 and DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 1
        group by 1
    );

drop table if exists cust_account_limit;
create temp table cust_account_limit distkey(consumer_uuid) as (
    select par_region
         , country_code
         , consumer_uuid
         , max(case when created_local_date = DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 2 then daily_account_limit end) as c_prior_credit_limit_2d
         , max(case when created_local_date = DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 3 then daily_account_limit end) as c_prior_credit_limit_3d
         , max(case when created_local_date = DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 10 then daily_account_limit end) as c_prior_credit_limit_10d
         , max(case when created_local_date = DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 30 then daily_account_limit end) as c_prior_credit_limit_30d
         , max(case when created_local_date = DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 90 then daily_account_limit end) as c_prior_credit_limit_90d
         , max(case when created_local_date = DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 180 then daily_account_limit end) as c_prior_credit_limit_180d
    from curated_feature_science_green.cust_limit_daily_prod_gb
    group by 1,2,3
);


create temp table cust_approved_order_d180 distkey(consumer_uuid) as (
    with tmp as (
        select *
        from green.raw_c_e_order
        where par_region IN ('GB')
          and par_process_date >= DATE(SYSDATE) - 180
          and status in ('APPROVED')
    )
    select consumer_consumer_uuid                                                                                 as consumer_uuid
         , sum(consumer_total_amount_amount::decimal(10, 2))                                                      as c_order_amt_180d
         , percent_rank() over (order by c_order_amt_180d)                                                        as pct_rank
         , case when pct_rank >= 0.99 then 1 else 100 - floor(100 * pct_rank) end                                 as c_order_amt_percentile_180d
    from tmp
    where DATE(green.fn_in_local_tz(country_code, TIMESTAMP 'epoch' + key_event_info_event_time / 1000 *
                                                                      interval '1 second')) between DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 180 and DATE(green.fn_in_local_tz(country_code, SYSDATE)) - 1
    group by 1
);

features:
c_tenure
par_region
consumer_id
consumer_uuid
country_code
par_process_date

Vega tables used:
green.d_consumer

create table sandbox_analytics_us.credit_model_revamp_cust_base_gb as(
    select uuid as consumer_uuid
         , id as consumer_id
         , country_code
         , par_region
         , case
               when first_order_date is null then 0
               else DATE(green.fn_in_local_tz(country_code, SYSDATE)) - DATE(first_order_date) end as c_tenure
    from green.d_consumer
    where par_region IN ('GB'));




