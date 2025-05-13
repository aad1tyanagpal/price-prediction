
TRUNCATE TABLE analytics_data.model1_jins_market_moving_avg;

WITH parameters AS (
    SELECT 'Anupgarh' AS selected_market, 'Mustard' AS selected_jins
),
moving_avg AS (
    SELECT 
        mdl.date AS reported_date,
        COALESCE(mdb1.market_name, (SELECT selected_market FROM parameters)) AS market_name,
        COALESCE(mdb1.jins_name, (SELECT selected_jins FROM parameters)) AS jins_name,
        mdb1.symbol,
        mdb1.max_price_rs_quintal AS max_price_from_mdb,
        mdb1.min_price_rs_quintal AS min_price_from_mdb,
        mdb1.max_raw AS raw_max_price_from_mdb,
        mdb1.min_raw AS raw_min_price_from_mdb,

    --Calculation of the future and past moving average MIN PRICES
        ROUND(AVG(CASE WHEN mdb2.min_price_rs_quintal IS NULL OR mdb2.min_price_rs_quintal = 0 OR mdb2.min_price_rs_quintal > 100000 THEN NULL ELSE mdb2.min_price_rs_quintal END) 
            FILTER (WHERE mdb2.reported_date BETWEEN mdl.date - INTERVAL '3 days' AND mdl.date - INTERVAL '1 day')::NUMERIC, 2) AS past_3_days_min_moving_average,
        ROUND(AVG(CASE WHEN mdb2.min_price_rs_quintal IS NULL OR mdb2.min_price_rs_quintal = 0 OR mdb2.min_price_rs_quintal > 100000 THEN NULL ELSE mdb2.min_price_rs_quintal END) 
            FILTER (WHERE mdb2.reported_date BETWEEN mdl.date - INTERVAL '5 days' AND mdl.date - INTERVAL '1 day')::NUMERIC, 2) AS past_5_days_min_moving_average,
        ROUND(AVG(CASE WHEN mdb2.min_price_rs_quintal IS NULL OR mdb2.min_price_rs_quintal = 0 OR mdb2.min_price_rs_quintal > 100000 THEN NULL ELSE mdb2.min_price_rs_quintal END) 
            FILTER (WHERE mdb2.reported_date BETWEEN mdl.date - INTERVAL '7 days' AND mdl.date - INTERVAL '1 day')::NUMERIC, 2) AS past_7_days_min_moving_average,
        ROUND(AVG(CASE WHEN mdb2.min_price_rs_quintal IS NULL OR mdb2.min_price_rs_quintal = 0 OR mdb2.min_price_rs_quintal > 100000 THEN NULL ELSE mdb2.min_price_rs_quintal END) 
            FILTER (WHERE mdb2.reported_date BETWEEN mdl.date - INTERVAL '10 days' AND mdl.date - INTERVAL '1 day')::NUMERIC, 2) AS past_10_days_min_moving_average,
        ROUND(AVG(CASE WHEN mdb2.min_price_rs_quintal IS NULL OR mdb2.min_price_rs_quintal = 0 OR mdb2.min_price_rs_quintal > 100000 THEN NULL ELSE mdb2.min_price_rs_quintal END) 
            FILTER (WHERE mdb2.reported_date BETWEEN mdl.date - INTERVAL '15 days' AND mdl.date - INTERVAL '1 day')::NUMERIC, 2) AS past_15_days_min_moving_average,
        ROUND(AVG(CASE WHEN mdb2.min_price_rs_quintal IS NULL OR mdb2.min_price_rs_quintal = 0 OR mdb2.min_price_rs_quintal > 100000 THEN NULL ELSE mdb2.min_price_rs_quintal END) 
            FILTER (WHERE mdb2.reported_date BETWEEN mdl.date - INTERVAL '20 days' AND mdl.date - INTERVAL '1 day')::NUMERIC, 2) AS past_20_days_min_moving_average,
        ROUND(AVG(CASE WHEN mdb2.min_price_rs_quintal IS NULL OR mdb2.min_price_rs_quintal = 0 OR mdb2.min_price_rs_quintal > 100000 THEN NULL ELSE mdb2.min_price_rs_quintal END) 
            FILTER (WHERE mdb2.reported_date BETWEEN mdl.date - INTERVAL '30 days' AND mdl.date - INTERVAL '1 day')::NUMERIC, 2) AS past_30_days_min_moving_average,
        ROUND(AVG(CASE WHEN mdb2.min_price_rs_quintal IS NULL OR mdb2.min_price_rs_quintal = 0 OR mdb2.min_price_rs_quintal > 100000 THEN NULL ELSE mdb2.min_price_rs_quintal END) 
            FILTER (WHERE mdb2.reported_date BETWEEN mdl.date - INTERVAL '45 days' AND mdl.date - INTERVAL '1 day')::NUMERIC, 2) AS past_45_days_min_moving_average,
        ROUND(AVG(CASE WHEN mdb2.min_price_rs_quintal IS NULL OR mdb2.min_price_rs_quintal = 0 OR mdb2.min_price_rs_quintal > 100000 THEN NULL ELSE mdb2.min_price_rs_quintal END) 
            FILTER (WHERE mdb2.reported_date BETWEEN mdl.date - INTERVAL '60 days' AND mdl.date - INTERVAL '1 day')::NUMERIC, 2) AS past_60_days_min_moving_average,
        ROUND(AVG(CASE WHEN mdb2.min_price_rs_quintal IS NULL OR mdb2.min_price_rs_quintal = 0 OR mdb2.min_price_rs_quintal > 100000 THEN NULL ELSE mdb2.min_price_rs_quintal END) 
            FILTER (WHERE mdb2.reported_date BETWEEN mdl.date - INTERVAL '75 days' AND mdl.date - INTERVAL '1 day')::NUMERIC, 2) AS past_75_days_min_moving_average,
        ROUND(AVG(CASE WHEN mdb2.min_price_rs_quintal IS NULL OR mdb2.min_price_rs_quintal = 0 OR mdb2.min_price_rs_quintal > 100000 THEN NULL ELSE mdb2.min_price_rs_quintal END) 
            FILTER (WHERE mdb2.reported_date BETWEEN mdl.date - INTERVAL '90 days' AND mdl.date - INTERVAL '1 day')::NUMERIC, 2) AS past_90_days_min_moving_average,
        ROUND(AVG(CASE WHEN mdb3.min_price_rs_quintal IS NULL OR mdb3.min_price_rs_quintal = 0 OR mdb3.min_price_rs_quintal > 100000 THEN NULL ELSE mdb3.min_price_rs_quintal END) 
            FILTER (WHERE mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '2 days')::NUMERIC, 2) AS future_3_days_min_moving_average,
        ROUND(AVG(CASE WHEN mdb3.min_price_rs_quintal IS NULL OR mdb3.min_price_rs_quintal = 0 OR mdb3.min_price_rs_quintal > 100000 THEN NULL ELSE mdb3.min_price_rs_quintal END) 
            FILTER (WHERE mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '4 days')::NUMERIC, 2) AS future_5_days_min_moving_average,
        ROUND(AVG(CASE WHEN mdb3.min_price_rs_quintal IS NULL OR mdb3.min_price_rs_quintal = 0 OR mdb3.min_price_rs_quintal > 100000 THEN NULL ELSE mdb3.min_price_rs_quintal END) 
            FILTER (WHERE mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '6 days')::NUMERIC, 2) AS future_7_days_min_moving_average,
        ROUND(AVG(CASE WHEN mdb3.min_price_rs_quintal IS NULL OR mdb3.min_price_rs_quintal = 0 OR mdb3.min_price_rs_quintal > 100000 THEN NULL ELSE mdb3.min_price_rs_quintal END) 
            FILTER (WHERE mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '9 days')::NUMERIC, 2) AS future_10_days_min_moving_average,
        ROUND(AVG(CASE WHEN mdb3.min_price_rs_quintal IS NULL OR mdb3.min_price_rs_quintal = 0 OR mdb3.min_price_rs_quintal > 100000 THEN NULL ELSE mdb3.min_price_rs_quintal END) 
            FILTER (WHERE mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '14 days')::NUMERIC, 2) AS future_15_days_min_moving_average,
        ROUND(AVG(CASE WHEN mdb3.min_price_rs_quintal IS NULL OR mdb3.min_price_rs_quintal = 0 OR mdb3.min_price_rs_quintal > 100000 THEN NULL ELSE mdb3.min_price_rs_quintal END) 
            FILTER (WHERE mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '19 days')::NUMERIC, 2) AS future_20_days_min_moving_average,
        ROUND(AVG(CASE WHEN mdb3.min_price_rs_quintal IS NULL OR mdb3.min_price_rs_quintal = 0 OR mdb3.min_price_rs_quintal > 100000 THEN NULL ELSE mdb3.min_price_rs_quintal END) 
            FILTER (WHERE mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '29 days')::NUMERIC, 2) AS future_30_days_min_moving_average,
        ROUND(AVG(CASE WHEN mdb3.min_price_rs_quintal IS NULL OR mdb3.min_price_rs_quintal = 0 OR mdb3.min_price_rs_quintal > 100000 THEN NULL ELSE mdb3.min_price_rs_quintal END) 
            FILTER (WHERE mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '44 days')::NUMERIC, 2) AS future_45_days_min_moving_average,
        ROUND(AVG(CASE WHEN mdb3.min_price_rs_quintal IS NULL OR mdb3.min_price_rs_quintal = 0 OR mdb3.min_price_rs_quintal > 100000 THEN NULL ELSE mdb3.min_price_rs_quintal END) 
            FILTER (WHERE mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '59 days')::NUMERIC, 2) AS future_60_days_min_moving_average,
        ROUND(AVG(CASE WHEN mdb3.min_price_rs_quintal IS NULL OR mdb3.min_price_rs_quintal = 0 OR mdb3.min_price_rs_quintal > 100000 THEN NULL ELSE mdb3.min_price_rs_quintal END) 
            FILTER (WHERE mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '74 days')::NUMERIC, 2) AS future_75_days_min_moving_average,
        ROUND(AVG(CASE WHEN mdb3.min_price_rs_quintal IS NULL OR mdb3.min_price_rs_quintal = 0 OR mdb3.min_price_rs_quintal > 100000 THEN NULL ELSE mdb3.min_price_rs_quintal END) 
            FILTER (WHERE mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '89 days')::NUMERIC, 2) AS future_90_days_min_moving_average,

        --Calculation of the future and past moving average MAX PRICES
        ROUND(AVG(CASE WHEN mdb2.max_price_rs_quintal IS NULL OR mdb2.max_price_rs_quintal = 0 OR mdb2.max_price_rs_quintal > 100000 THEN NULL ELSE mdb2.max_price_rs_quintal END) 
            FILTER (WHERE mdb2.reported_date BETWEEN mdl.date - INTERVAL '3 days' AND mdl.date - INTERVAL '1 day')::NUMERIC, 2) AS past_3_days_max_moving_average,
        ROUND(AVG(CASE WHEN mdb2.max_price_rs_quintal IS NULL OR mdb2.max_price_rs_quintal = 0 OR mdb2.max_price_rs_quintal > 100000 THEN NULL ELSE mdb2.max_price_rs_quintal END) 
            FILTER (WHERE mdb2.reported_date BETWEEN mdl.date - INTERVAL '5 days' AND mdl.date - INTERVAL '1 day')::NUMERIC, 2) AS past_5_days_max_moving_average,
        ROUND(AVG(CASE WHEN mdb2.max_price_rs_quintal IS NULL OR mdb2.max_price_rs_quintal = 0 OR mdb2.max_price_rs_quintal > 100000 THEN NULL ELSE mdb2.max_price_rs_quintal END) 
            FILTER (WHERE mdb2.reported_date BETWEEN mdl.date - INTERVAL '7 days' AND mdl.date - INTERVAL '1 day')::NUMERIC, 2) AS past_7_days_max_moving_average,
        ROUND(AVG(CASE WHEN mdb2.max_price_rs_quintal IS NULL OR mdb2.max_price_rs_quintal = 0 OR mdb2.max_price_rs_quintal > 100000 THEN NULL ELSE mdb2.max_price_rs_quintal END) 
            FILTER (WHERE mdb2.reported_date BETWEEN mdl.date - INTERVAL '10 days' AND mdl.date - INTERVAL '1 day')::NUMERIC, 2) AS past_10_days_max_moving_average,
        ROUND(AVG(CASE WHEN mdb2.max_price_rs_quintal IS NULL OR mdb2.max_price_rs_quintal = 0 OR mdb2.max_price_rs_quintal > 100000 THEN NULL ELSE mdb2.max_price_rs_quintal END) 
            FILTER (WHERE mdb2.reported_date BETWEEN mdl.date - INTERVAL '15 days' AND mdl.date - INTERVAL '1 day')::NUMERIC, 2) AS past_15_days_max_moving_average,
        ROUND(AVG(CASE WHEN mdb2.max_price_rs_quintal IS NULL OR mdb2.max_price_rs_quintal = 0 OR mdb2.max_price_rs_quintal > 100000 THEN NULL ELSE mdb2.max_price_rs_quintal END) 
            FILTER (WHERE mdb2.reported_date BETWEEN mdl.date - INTERVAL '20 days' AND mdl.date - INTERVAL '1 day')::NUMERIC, 2) AS past_20_days_max_moving_average,
        ROUND(AVG(CASE WHEN mdb2.max_price_rs_quintal IS NULL OR mdb2.max_price_rs_quintal = 0 OR mdb2.max_price_rs_quintal > 100000 THEN NULL ELSE mdb2.max_price_rs_quintal END) 
            FILTER (WHERE mdb2.reported_date BETWEEN mdl.date - INTERVAL '30 days' AND mdl.date - INTERVAL '1 day')::NUMERIC, 2) AS past_30_days_max_moving_average,
        ROUND(AVG(CASE WHEN mdb2.max_price_rs_quintal IS NULL OR mdb2.max_price_rs_quintal = 0 OR mdb2.max_price_rs_quintal > 100000 THEN NULL ELSE mdb2.max_price_rs_quintal END) 
            FILTER (WHERE mdb2.reported_date BETWEEN mdl.date - INTERVAL '45 days' AND mdl.date - INTERVAL '1 day')::NUMERIC, 2) AS past_45_days_max_moving_average,
        ROUND(AVG(CASE WHEN mdb2.max_price_rs_quintal IS NULL OR mdb2.max_price_rs_quintal = 0 OR mdb2.max_price_rs_quintal > 100000 THEN NULL ELSE mdb2.max_price_rs_quintal END) 
            FILTER (WHERE mdb2.reported_date BETWEEN mdl.date - INTERVAL '60 days' AND mdl.date - INTERVAL '1 day')::NUMERIC, 2) AS past_60_days_max_moving_average,
        ROUND(AVG(CASE WHEN mdb2.max_price_rs_quintal IS NULL OR mdb2.max_price_rs_quintal = 0 OR mdb2.max_price_rs_quintal > 100000 THEN NULL ELSE mdb2.max_price_rs_quintal END) 
            FILTER (WHERE mdb2.reported_date BETWEEN mdl.date - INTERVAL '75 days' AND mdl.date - INTERVAL '1 day')::NUMERIC, 2) AS past_75_days_max_moving_average,
        ROUND(AVG(CASE WHEN mdb2.max_price_rs_quintal IS NULL OR mdb2.max_price_rs_quintal = 0 OR mdb2.max_price_rs_quintal > 100000 THEN NULL ELSE mdb2.max_price_rs_quintal END) 
            FILTER (WHERE mdb2.reported_date BETWEEN mdl.date - INTERVAL '90 days' AND mdl.date - INTERVAL '1 day')::NUMERIC, 2) AS past_90_days_max_moving_average,
        ROUND(AVG(CASE WHEN mdb3.max_price_rs_quintal IS NULL OR mdb3.max_price_rs_quintal = 0 OR mdb3.max_price_rs_quintal > 100000 THEN NULL ELSE mdb3.max_price_rs_quintal END) 
            FILTER (WHERE mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '2 days')::NUMERIC, 2) AS future_3_days_max_moving_average,
        ROUND(AVG(CASE WHEN mdb3.max_price_rs_quintal IS NULL OR mdb3.max_price_rs_quintal = 0 OR mdb3.max_price_rs_quintal > 100000 THEN NULL ELSE mdb3.max_price_rs_quintal END) 
            FILTER (WHERE mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '4 days')::NUMERIC, 2) AS future_5_days_max_moving_average,
        ROUND(AVG(CASE WHEN mdb3.max_price_rs_quintal IS NULL OR mdb3.max_price_rs_quintal = 0 OR mdb3.max_price_rs_quintal > 100000 THEN NULL ELSE mdb3.max_price_rs_quintal END) 
            FILTER (WHERE mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '6 days')::NUMERIC, 2) AS future_7_days_max_moving_average,
        ROUND(AVG(CASE WHEN mdb3.max_price_rs_quintal IS NULL OR mdb3.max_price_rs_quintal = 0 OR mdb3.max_price_rs_quintal > 100000 THEN NULL ELSE mdb3.max_price_rs_quintal END) 
            FILTER (WHERE mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '9 days')::NUMERIC, 2) AS future_10_days_max_moving_average,
        ROUND(AVG(CASE WHEN mdb3.max_price_rs_quintal IS NULL OR mdb3.max_price_rs_quintal = 0 OR mdb3.max_price_rs_quintal > 100000 THEN NULL ELSE mdb3.max_price_rs_quintal END) 
            FILTER (WHERE mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '14 days')::NUMERIC, 2) AS future_15_days_max_moving_average,
        ROUND(AVG(CASE WHEN mdb3.max_price_rs_quintal IS NULL OR mdb3.max_price_rs_quintal = 0 OR mdb3.max_price_rs_quintal > 100000 THEN NULL ELSE mdb3.max_price_rs_quintal END) 
            FILTER (WHERE mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '19 days')::NUMERIC, 2) AS future_20_days_max_moving_average,
        ROUND(AVG(CASE WHEN mdb3.max_price_rs_quintal IS NULL OR mdb3.max_price_rs_quintal = 0 OR mdb3.max_price_rs_quintal > 100000 THEN NULL ELSE mdb3.max_price_rs_quintal END) 
            FILTER (WHERE mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '29 days')::NUMERIC, 2) AS future_30_days_max_moving_average,
        ROUND(AVG(CASE WHEN mdb3.max_price_rs_quintal IS NULL OR mdb3.max_price_rs_quintal = 0 OR mdb3.max_price_rs_quintal > 100000 THEN NULL ELSE mdb3.max_price_rs_quintal END) 
            FILTER (WHERE mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '44 days')::NUMERIC, 2) AS future_45_days_max_moving_average,
        ROUND(AVG(CASE WHEN mdb3.max_price_rs_quintal IS NULL OR mdb3.max_price_rs_quintal = 0 OR mdb3.max_price_rs_quintal > 100000 THEN NULL ELSE mdb3.max_price_rs_quintal END) 
            FILTER (WHERE mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '59 days')::NUMERIC, 2) AS future_60_days_max_moving_average,
        ROUND(AVG(CASE WHEN mdb3.max_price_rs_quintal IS NULL OR mdb3.max_price_rs_quintal = 0 OR mdb3.max_price_rs_quintal > 100000 THEN NULL ELSE mdb3.max_price_rs_quintal END) 
            FILTER (WHERE mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '74 days')::NUMERIC, 2) AS future_75_days_max_moving_average,
        ROUND(AVG(CASE WHEN mdb3.max_price_rs_quintal IS NULL OR mdb3.max_price_rs_quintal = 0 OR mdb3.max_price_rs_quintal > 100000 THEN NULL ELSE mdb3.max_price_rs_quintal END) 
            FILTER (WHERE mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '89 days')::NUMERIC, 2) AS future_90_days_max_moving_average

    FROM mandi_data.mandi_date_log mdl
    LEFT JOIN mandi_data.mandi_mdb mdb1 
        ON mdl.date = mdb1.reported_date AND mdb1.jins_name = (SELECT selected_jins FROM parameters) AND mdb1.market_name = (SELECT selected_market FROM parameters)
    LEFT JOIN mandi_data.mandi_mdb mdb2 
        ON mdb2.jins_name = (SELECT selected_jins FROM parameters) AND mdb2.market_name = (SELECT selected_market FROM parameters) AND mdb2.reported_date BETWEEN mdl.date - INTERVAL '90 days' AND mdl.date - INTERVAL '1 day'
    LEFT JOIN mandi_data.mandi_mdb mdb3 
        ON mdb3.jins_name = (SELECT selected_jins FROM parameters) AND mdb3.market_name = (SELECT selected_market FROM parameters) AND mdb3.reported_date BETWEEN mdl.date AND mdl.date + INTERVAL '89 days'
    WHERE mdl.date_processed = 1
    GROUP BY mdl.date, mdb1.market_name, mdb1.jins_name, mdb1.symbol, mdb1.max_price_rs_quintal, mdb1.min_price_rs_quintal, mdb1.max_raw, mdb1.min_raw
)

-- Insert the calculated moving averages
INSERT INTO analytics_data.model1_jins_market_moving_avg (
    reported_date, market_name, jins_name, symbol, date_time_stamp, max_price_from_mdb, min_price_from_mdb, raw_max_price_from_mdb, raw_min_price_from_mdb, 
    past_3_days_min_moving_average, future_3_days_min_moving_average, 
    past_5_days_min_moving_average, future_5_days_min_moving_average,
    past_7_days_min_moving_average, future_7_days_min_moving_average,
    past_10_days_min_moving_average, future_10_days_min_moving_average,
    past_15_days_min_moving_average, future_15_days_min_moving_average,
    past_20_days_min_moving_average, future_20_days_min_moving_average,
    past_30_days_min_moving_average, future_30_days_min_moving_average,
    past_45_days_min_moving_average, future_45_days_min_moving_average,
    past_60_days_min_moving_average, future_60_days_min_moving_average,
    past_75_days_min_moving_average, future_75_days_min_moving_average,
    past_90_days_min_moving_average, future_90_days_min_moving_average,
    past_3_days_max_moving_average, future_3_days_max_moving_average, 
    past_5_days_max_moving_average, future_5_days_max_moving_average,
    past_7_days_max_moving_average, future_7_days_max_moving_average,
    past_10_days_max_moving_average, future_10_days_max_moving_average,
    past_15_days_max_moving_average, future_15_days_max_moving_average,
    past_20_days_max_moving_average, future_20_days_max_moving_average,
    past_30_days_max_moving_average, future_30_days_max_moving_average,
    past_45_days_max_moving_average, future_45_days_max_moving_average,
    past_60_days_max_moving_average, future_60_days_max_moving_average,
    past_75_days_max_moving_average, future_75_days_max_moving_average,
    past_90_days_max_moving_average, future_90_days_max_moving_average
)
SELECT 
    reported_date, market_name, jins_name, symbol, NOW(), max_price_from_mdb, min_price_from_mdb, raw_max_price_from_mdb, raw_min_price_from_mdb, 
    past_3_days_min_moving_average, future_3_days_min_moving_average,
    past_5_days_min_moving_average, future_5_days_min_moving_average,
    past_7_days_min_moving_average, future_7_days_min_moving_average,
    past_10_days_min_moving_average, future_10_days_min_moving_average,
    past_15_days_min_moving_average, future_15_days_min_moving_average,
    past_20_days_min_moving_average, future_20_days_min_moving_average,
    past_30_days_min_moving_average, future_30_days_min_moving_average,
    past_45_days_min_moving_average, future_45_days_min_moving_average,
    past_60_days_min_moving_average, future_60_days_min_moving_average,
    past_75_days_min_moving_average, future_75_days_min_moving_average,
    past_90_days_min_moving_average, future_90_days_min_moving_average,
    past_3_days_max_moving_average, future_3_days_max_moving_average,
    past_5_days_max_moving_average, future_5_days_max_moving_average,
    past_7_days_max_moving_average, future_7_days_max_moving_average,
    past_10_days_max_moving_average, future_10_days_max_moving_average,
    past_15_days_max_moving_average, future_15_days_max_moving_average,
    past_20_days_max_moving_average, future_20_days_max_moving_average,
    past_30_days_max_moving_average, future_30_days_max_moving_average,
    past_45_days_max_moving_average, future_45_days_max_moving_average,
    past_60_days_max_moving_average, future_60_days_max_moving_average,
    past_75_days_max_moving_average, future_75_days_max_moving_average,
    past_90_days_max_moving_average, future_90_days_max_moving_average
FROM moving_avg;