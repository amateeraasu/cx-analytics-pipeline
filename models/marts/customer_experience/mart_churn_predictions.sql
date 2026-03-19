-- Churn predictions enriched with customer context.
-- PREREQUISITE: Run notebooks/churn_prediction.ipynb first to generate data/churn_predictions.csv.
with predictions as (
    select *
    from read_csv_auto(
        'data/churn_predictions.csv',
        columns = {
            'customer_unique_id':          'VARCHAR',
            'churn_probability':           'DOUBLE',
            'predicted_label':             'INTEGER',
            'model_name':                  'VARCHAR',
            'prediction_generated_at':     'VARCHAR'
        }
    )
),

enriched as (
    select
        p.customer_unique_id,
        p.churn_probability,
        p.predicted_label,
        p.model_name,
        p.prediction_generated_at::TIMESTAMP as prediction_generated_at,

        -- Risk tier for prioritisation
        case
            when p.churn_probability >= 0.85 then 'critical'
            when p.churn_probability >= 0.65 then 'high'
            when p.churn_probability >= 0.40 then 'medium'
            else 'low'
        end as churn_risk_tier,

        -- Customer context
        d.state,
        d.order_frequency_segment,
        d.satisfaction_segment,
        d.total_orders,
        d.total_spend_brl,
        d.avg_order_value_brl,
        d.avg_review_score,
        d.avg_days_to_deliver,
        d.first_order_at,
        d.last_order_at,
        d.customer_lifespan_days

    from predictions p
    left join {{ ref('dim_customers') }} d using (customer_unique_id)
)

select * from enriched
