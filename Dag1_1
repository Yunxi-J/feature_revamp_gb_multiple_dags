vega tables used:
-- green.raw_p_e_payment_scheduled
-- green.raw_c_e_order
-- green.raw_p_e_pymt_schd_due_dt_changed
-- green.raw_p_e_pymt_prcsd_instlmnt_pymt
-- green.raw_p_e_payment_processed
-- raw_p_e_pymt_schd_became_overdue
-- green.raw_p_e_payment_schdl_override


-- To create intermediate table
-- sandbox_analytics_us.instal_events_agg_gb;

-- This script will be used in latepayment&ontime_payment and latepayment_fee
-- and the final merge


DROP TABLE IF EXISTS sandbox_analytics_us.instal_invoice;
CREATE TABLE sandbox_analytics_us.instal_invoice distkey(instalment_id) AS (
    SELECT *
         , ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY instalment_id ASC) AS instalment_seq_id
    FROM (
             SELECT DISTINCT orders.consumer_consumer_uuid                                                      AS consumer_uuid
                           , orders.country_code                                                                AS country_code
                           , base.key_payment_schedule_id                                                       AS instalment_id
                           , DATE(base.due_date_string)                                                         AS original_due_date
                           , base.amount_amount::DECIMAL(20, 2)                                                 AS inv_amount
                           , base.event_info_event_time                                                         AS invoice_event_time
                           , base.par_region
                           , green.fn_in_local_tz(orders.country_code, TIMESTAMP 'epoch' + invoice_event_time / 1000 *
                                                                                           interval '1 second') AS invoice_event_datetime
                           , orders.order_transaction_id                                                        AS order_id
                           , green.fn_in_local_tz(orders.country_code, TIMESTAMP 'epoch' +
                                                                       key_event_info_event_time / 1000 *
                                                                       interval '1 second')                     AS order_event_datetime
                           , orders.first_payment_up_front
                           , orders.consumer_total_amount_amount::DECIMAL(20, 2)                                AS consumer_amount
             FROM green.raw_p_e_payment_scheduled base
                      INNER JOIN green.raw_c_e_order orders
                                 ON orders.order_transaction_id = base.order_transaction_id
                                     AND orders.par_region = base.par_region
             WHERE orders.status IN ('APPROVED')
               AND base.par_region IN ('GB')
               AND orders.par_region IN ('GB')
               AND base.par_process_date >= DATE(SYSDATE) - 450
               AND orders.par_process_date >= DATE(SYSDATE) - 450
         ) AS AA
);

select 1;
select * from sandbox_analytics_us.instal_invoice limit 100;

DROP TABLE IF EXISTS sandbox_analytics_us.instalment_due_date_history;
CREATE TABLE sandbox_analytics_us.instalment_due_date_history AS (
    SELECT base.key_consumer_consumer_uuid AS consumer_uuid
         , base.key_payment_schedule_id AS instalment_id
         , base.order_transaction_id AS order_id
         , instal.country_code
         , green.fn_in_local_tz(instal.country_code, TIMESTAMP 'epoch' + base.event_info_event_time/1000 * interval '1 second') AS effective_datetime
         , DATE(effective_datetime) AS effective_date
         , DATE(base.old_due_date_string) AS prev_due_date
         , DATE(base.new_due_date_string) AS due_date
         , CASE WHEN effective_date > prev_due_date THEN 1 ELSE 0 END invalid_flag
    FROM green.raw_p_e_pymt_schd_due_dt_changed base
             INNER JOIN sandbox_analytics_us.instal_invoice instal
                        ON instal.instalment_id = base.key_payment_schedule_id
                            AND instal.order_id = base.order_transaction_id
    WHERE base.par_process_date >= DATE(SYSDATE) - 450
      AND base.par_region in ('GB')
      AND (COALESCE(base.old_due_date_string,'') <> '')
      AND (COALESCE(base.new_due_date_string,'') <> '')
);

select * from sandbox_analytics_us.instalment_due_date_history limit 100;

DROP TABLE IF EXISTS sandbox_analytics_us.instalment_due_date_valid;
CREATE TABLE sandbox_analytics_us.instalment_due_date_valid distkey(instalment_id) AS (
    SELECT base.*
    FROM (SELECT *
               , ROW_NUMBER() OVER(PARTITION BY instalment_id, order_id ORDER BY effective_datetime DESC) AS _dedup
          FROM sandbox_analytics_us.instalment_due_date_history
         )base
    WHERE _dedup = 1
);

select * from sandbox_analytics_us.instalment_due_date_valid limit 100;


-- instalment events level
DROP TABLE IF EXISTS instal_events; -- instalment events level
CREATE TEMP TABLE instal_events AS (
    SELECT  instal.consumer_uuid
         , instal.country_code
         , p.key_payment_schedule_id AS instalment_id
         , instal.order_id
         , instal.par_region
         , CASE WHEN COALESCE(pp.payment_method_discount_name,'') = '' THEN 'PAYMENT'
                WHEN COALESCE(pp.payment_method_discount_name,'') <> '' THEN 'DISCOUNT'
                ELSE 'other' END AS event_type
         , p.key_payment_id AS payment_id
         , NULL::BIGINT AS refund_id
         , p.event_time
         , p.amount_amount::DECIMAL(20,2) AS amount_paid
         , 0::DECIMAL(20,2) AS amount_waived
         , p.payment_schedule_status
    FROM green.raw_p_e_pymt_prcsd_instlmnt_pymt p
             INNER JOIN (
        SELECT key_consumer_consumer_uuid as consumer_uuid
             , key_payment_id
             , par_region
             , payment_method_discount_name
             , event_info_event_time
             , ROW_NUMBER() over (partition by key_payment_id order by event_info_event_time desc) as _dedup
        FROM green.raw_p_e_payment_processed
        WHERE payment_status IN ('SUCCESSFUL')
          AND par_region IN ('GB')
          AND par_process_date >= DATE(SYSDATE) - 450
    )pp
                        ON pp.key_payment_id = p.key_payment_id
                            AND pp._dedup = 1
                            AND pp.event_info_event_time = p.event_time
             INNER JOIN (select consumer_uuid, country_code, instalment_id, order_id, par_region from sandbox_analytics_us.instal_invoice)  instal
                        ON instal.instalment_id = p.key_payment_schedule_id
    WHERE p.par_region IN ('US')
      AND p.par_process_date >= DATE(SYSDATE) - 450
    group by 1,2,3,4,5,6,7,8,9,10,11,12
);

select * from sandbox_analytics_us.instal_events limit 100;

DROP TABLE IF EXISTS sandbox_analytics_us.instal_events_agg_gb;
CREATE TABLE sandbox_analytics_us.instal_events_agg_gb distkey(instalment_id) AS (
    SELECT base.instalment_id
         , base.consumer_uuid
         , base.country_code
         , base.par_region
         , base.inv_amount
         , base.invoice_event_datetime
         , base.order_id
         , base.order_event_datetime
         , base.first_payment_up_front
         , base.instalment_seq_id
         , case
               when base.first_payment_up_front = true and base.instalment_seq_id = 1 then 1
               else 0 end                                                                         as upfront_instalment
         , base.original_due_date
         , COALESCE(dt.due_date, base.original_due_date)                                          AS current_due_date
         , SUM(case
                   when COALESCE(events.payment_schedule_status, '') = 'PAID' and COALESCE(events.event_type, '') = 'PAYMENT'
                       then 1
                   else 0 end)                                                                    as paid_cnt
         , SUM(case when COALESCE(events.event_type, '') = 'PAYMENT' then 1 else 0 end)                      as payment_cnt
         , SUM(case when COALESCE(events.event_type, '') = 'REFUNDED' then 1 else 0 end)                     as waived_cnt
         , MAX(CASE WHEN COALESCE(events.event_type, '') = 'PAYMENT' THEN events.event_time END)             AS latest_instal_paid_datetime
         , MAX(CASE WHEN COALESCE(events.event_type, '') = 'REFUNDED' THEN events.event_time END)            AS latest_instal_waived_datetime
         , DATE(green.fn_in_local_tz(base.country_code, TIMESTAMP 'epoch' + latest_instal_paid_datetime / 1000 *
                                                                            interval '1 second')) as latest_instal_paid_date
         , DATE(green.fn_in_local_tz(base.country_code, TIMESTAMP 'epoch' + latest_instal_waived_datetime / 1000 *
                                                                            interval '1 second')) as latest_instal_waived_date
         , SUM(CASE WHEN COALESCE(events.event_type, '') = 'REFUNDED' THEN events.amount_waived ELSE 0 END) AS waived_amount
         , SUM(CASE WHEN COALESCE(events.event_type, '') = 'DISCOUNT' THEN events.amount_paid ELSE 0 END) AS discount_amount
         , case when inv_amount - discount_amount + waived_amount <= 0 then 1 else 0 end as full_waived
    FROM sandbox_analytics_us.instal_invoice base
             LEFT JOIN sandbox_analytics_us.instal_events events
                       ON events.instalment_id = base.instalment_id
                           AND events.order_id = base.order_id
                           AND events.consumer_uuid = base.consumer_uuid
                           AND events.event_type IN ('PAYMENT', 'REFUNDED', 'DISCOUNT')
                           AND DATE(green.fn_in_local_tz(base.country_code, TIMESTAMP 'epoch' + events.event_time / 1000 *
                                                                                                interval '1 second')) between DATE(green.fn_in_local_tz(base.country_code, SYSDATE)) - 360 and DATE(green.fn_in_local_tz(base.country_code, SYSDATE)) - 1
             LEFT JOIN sandbox_analytics_us.instalment_due_date_valid dt
                       ON base.instalment_id = dt.instalment_id
                           AND base.order_id = dt.order_id
    WHERE DATE(current_due_date) >= DATE(green.fn_in_local_tz(base.country_code, SYSDATE)) - 360
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
);

select * from sandbox_analytics_us.instal_events_agg_gb  limit 100;

