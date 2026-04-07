{{ config(materialized='incremental', unique_key='order_id', alias='orders_with_last_touch_timestamp') }}

with orders_and_events as (select o.*,
                                  e.event_timestamp,
                                  e.event_name
                           from raw.finelo_orders as o
                                    left join raw.finelo_funnel_events as e
                                              on e.customer_account_id = o.customer_account_id
                                                  and e.event_timestamp < o.created_at
                           {% if is_incremental() %} -- Инкрементальная загрузка, исключает те платежи что уже попали в витрину, и включает те что обновились, делает merge по order_id
                           where o.updated_at > (select COALESCE(max(updated_at), cast('1970-01-01' as timestamp))
                                                 FROM {{ this }})
                           {% endif %})
select order_id,
       status,
       type,
       amount,
       currency,
       processing_amount,
       processing_currency,
       customer_account_id,
       geo_country,
       error_code,
       platform,
       fraudulent,
       is_secured,
       created_at,
       updated_at,
       mid,
       json_value(replace(routing, "'", '"'), '$.cascade_steps[0].route_id') as route_id,
       max(case
               when event_name = 'pr_funnel_landing_view'
                   then event_timestamp
           end)                                                              as last_pr_funnel_landing_view,
       max(case
               when event_name = 'pr_funnel_quiz_question_answer'
                   then event_timestamp
           end)                                                              as last_pr_funnel_quiz_question_answer,
       max(case
               when event_name = 'pr_funnel_email_submit'
                   then event_timestamp
           end)                                                              as last_pr_funnel_email_submit,
       max(case
               when event_name = 'pr_funnel_selling_page_view'
                   then event_timestamp
           end)                                                              as last_pr_funnel_selling_page_view,
       max(case
               when event_name = 'pr_funnel_paywall_true_purchase_click'
                   then event_timestamp
           end)                                                              as last_pr_funnel_paywall_true_purchase_click,
       max(case
               when event_name = 'pr_funnel_subscribe'
                   then event_timestamp
           end)                                                              as last_pr_funnel_subscribe
from orders_and_events
group by order_id,
         status,
         type,
         amount,
         currency,
         processing_amount,
         processing_currency,
         customer_account_id,
         geo_country,
         error_code,
         platform,
         fraudulent,
         is_secured,
         created_at,
         updated_at,
         mid,
         json_value(replace(routing, "'", '"'), '$.cascade_steps[0].route_id')