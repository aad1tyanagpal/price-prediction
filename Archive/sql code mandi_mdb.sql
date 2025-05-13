 

-- Step 0: Define the year as a global variable 

WITH global_vars AS ( 

  SELECT 2024 AS year -- You can change the year here 

), 

-- Step 1: Define the price_data CTE to clean and prepare raw data 

price_data AS ( 

  SELECT  

      mrm.reported_date,  

      mrm.state_name,  

      mrm.district_name,  

      mrm.market_name,  

      mrm.variety,  

      mrm.family,  

      mrm.jins_name,  

      mrm.jins_code,  

      mg.geo_symbol AS geo_symbol,  

      mjs.jins_symbol,  

      mg.geo_symbol || mjs.jins_symbol AS symbol, 

      TO_CHAR(mrm.reported_date, 'YYYYMMDD') || mg.geo_symbol || mjs.jins_symbol AS uid, 

      mrm.arrivals_tonnes,   

      mrm.min_price_rs_quintal, 

      mrm.max_price_rs_quintal, 

      mrm.modal_price_rs_quintal, 

      -- Determine the initial price values where at least two of the three prices are zero 

      -- Choose non-zero price as the initial price value where possible 

      -- For initial_min_price_rs_quintal 

      CASE 

          WHEN (mrm.min_price_rs_quintal IS NULL OR mrm.min_price_rs_quintal = 0)  

               AND (mrm.max_price_rs_quintal IS NULL OR mrm.max_price_rs_quintal = 0) 

               AND (mrm.modal_price_rs_quintal IS NOT NULL AND mrm.modal_price_rs_quintal != 0)  

          THEN mrm.modal_price_rs_quintal 

          WHEN (mrm.min_price_rs_quintal IS NULL OR mrm.min_price_rs_quintal = 0) 

               AND (mrm.max_price_rs_quintal IS NOT NULL AND mrm.max_price_rs_quintal != 0) 

               AND (mrm.modal_price_rs_quintal IS NULL OR mrm.modal_price_rs_quintal = 0) 

          THEN mrm.max_price_rs_quintal 

      END AS initial_min_price_rs_quintal, 

      -- For initial_max_price_rs_quintal 

      CASE 

          WHEN (mrm.min_price_rs_quintal IS NULL OR mrm.min_price_rs_quintal = 0)  

               AND (mrm.max_price_rs_quintal IS NULL OR mrm.max_price_rs_quintal = 0) 

               AND (mrm.modal_price_rs_quintal IS NOT NULL AND mrm.modal_price_rs_quintal != 0)  

          THEN mrm.modal_price_rs_quintal 

          WHEN (mrm.min_price_rs_quintal IS NOT NULL AND mrm.min_price_rs_quintal != 0) 

               AND (mrm.max_price_rs_quintal IS NULL OR mrm.max_price_rs_quintal = 0) 

               AND (mrm.modal_price_rs_quintal IS NULL OR mrm.modal_price_rs_quintal = 0) 

          THEN mrm.min_price_rs_quintal 

      END AS initial_max_price_rs_quintal, 

      -- For initial_modal_price_rs_quintal 

      CASE 

          WHEN (mrm.min_price_rs_quintal IS NOT NULL AND mrm.min_price_rs_quintal != 0) 

               AND (mrm.max_price_rs_quintal IS NULL OR mrm.max_price_rs_quintal = 0) 

               AND (mrm.modal_price_rs_quintal IS NULL OR mrm.modal_price_rs_quintal = 0) 

          THEN mrm.min_price_rs_quintal 

          WHEN (mrm.min_price_rs_quintal IS NULL OR mrm.min_price_rs_quintal = 0) 

               AND (mrm.max_price_rs_quintal IS NOT NULL AND mrm.max_price_rs_quintal != 0) 

               AND (mrm.modal_price_rs_quintal IS NULL OR mrm.modal_price_rs_quintal = 0) 

          THEN mrm.max_price_rs_quintal 

      END AS initial_modal_price_rs_quintal, 

      -- Correct prices where only one of the values is zero 

      -- Adjust the zero value based on the relationship between the remaining values 

      CASE 

          WHEN (mrm.min_price_rs_quintal IS NULL OR mrm.min_price_rs_quintal = 0)  

               AND (mrm.max_price_rs_quintal IS NOT NULL AND mrm.max_price_rs_quintal != 0)  

               AND (mrm.modal_price_rs_quintal IS NOT NULL AND mrm.modal_price_rs_quintal != 0) 

          THEN (mrm.modal_price_rs_quintal * 2 - mrm.max_price_rs_quintal) 

      END AS corrected_min_price_rs_quintal, 

      CASE 

          WHEN (mrm.min_price_rs_quintal IS NOT NULL AND mrm.min_price_rs_quintal != 0)  

               AND (mrm.max_price_rs_quintal IS NULL OR mrm.max_price_rs_quintal = 0)  

               AND (mrm.modal_price_rs_quintal IS NOT NULL AND mrm.modal_price_rs_quintal != 0) 

          THEN (mrm.modal_price_rs_quintal * 2 - mrm.min_price_rs_quintal) 

      END AS corrected_max_price_rs_quintal, 

      CASE 

          WHEN (mrm.min_price_rs_quintal IS NOT NULL AND mrm.min_price_rs_quintal != 0)  

               AND (mrm.max_price_rs_quintal IS NOT NULL AND mrm.max_price_rs_quintal != 0)  

               AND (mrm.modal_price_rs_quintal IS NULL OR mrm.modal_price_rs_quintal = 0) 

          THEN ((mrm.max_price_rs_quintal + mrm.min_price_rs_quintal) / 2) 

      END AS corrected_modal_price_rs_quintal 

  FROM  

      mandi_data.mandi_raw_master mrm 

  JOIN  

      mandi_data.mandi_geosymbol mg  

  ON  

      mrm.state_name = mg.state_name  

      AND mrm.district_name = mg.district_name  

      AND mrm.market_name = mg.market_name 

  JOIN  

      mandi_data.mandi_jinssymbol mjs  

  ON  

      mrm.jins_name = mjs.jins_name 

  CROSS JOIN 

      global_vars 

  WHERE 

      EXTRACT(YEAR FROM mrm.reported_date) = global_vars.year 

), 

-- Step 2: Calculate final raw prices in a separate CTE 

final_raw_price_data AS ( 

  SELECT  

      pd.*, 

      -- Final raw prices after applying the condition on COALESCE 

      -- Scale down prices if they are excessively large 

      CASE  

          WHEN COALESCE(pd.corrected_max_price_rs_quintal, pd.initial_max_price_rs_quintal, pd.max_price_rs_quintal) >= 100000  

               AND COALESCE(pd.corrected_max_price_rs_quintal, pd.initial_max_price_rs_quintal, pd.max_price_rs_quintal) <= 10000000  

          THEN COALESCE(pd.corrected_max_price_rs_quintal, pd.initial_max_price_rs_quintal, pd.max_price_rs_quintal) / 100  

          ELSE COALESCE(pd.corrected_max_price_rs_quintal, pd.initial_max_price_rs_quintal, pd.max_price_rs_quintal) 

      END AS final_raw_max_price_rs_quintal, 

      CASE  

          WHEN COALESCE(pd.corrected_min_price_rs_quintal, pd.initial_min_price_rs_quintal, pd.min_price_rs_quintal) >= 100000  

               AND COALESCE(pd.corrected_min_price_rs_quintal, pd.initial_min_price_rs_quintal, pd.min_price_rs_quintal) <= 10000000  

          THEN COALESCE(pd.corrected_min_price_rs_quintal, pd.initial_min_price_rs_quintal, pd.min_price_rs_quintal) / 100  

          ELSE COALESCE(pd.corrected_min_price_rs_quintal, pd.initial_min_price_rs_quintal, pd.min_price_rs_quintal) 

      END AS final_raw_min_price_rs_quintal, 

      CASE  

          WHEN COALESCE(pd.corrected_modal_price_rs_quintal, pd.initial_modal_price_rs_quintal, pd.modal_price_rs_quintal) >= 100000  

               AND COALESCE(pd.corrected_modal_price_rs_quintal, pd.initial_modal_price_rs_quintal, pd.modal_price_rs_quintal) <= 10000000  

          THEN COALESCE(pd.corrected_modal_price_rs_quintal, pd.initial_modal_price_rs_quintal, pd.modal_price_rs_quintal) / 100  

          ELSE COALESCE(pd.corrected_modal_price_rs_quintal, pd.initial_modal_price_rs_quintal, pd.modal_price_rs_quintal) 

      END AS final_raw_modal_price_rs_quintal 

  FROM price_data pd 

), 

-- Step 3: Calculate moving averages for the past 14 days excluding the reported date 

moving_avg AS ( 

  SELECT  

      frpd1.reported_date, 

      frpd1.state_name, 

      frpd1.district_name, 

      frpd1.market_name, 

      frpd1.variety, 

      frpd1.symbol, 

      frpd1.uid, 

      -- Compute moving average for min price 

      ROUND( 

          AVG( 

              CASE  

                  WHEN frpd2.final_raw_min_price_rs_quintal IS NULL  

                       OR frpd2.final_raw_min_price_rs_quintal = 0  

                       OR frpd2.final_raw_min_price_rs_quintal > 100000 THEN NULL 

                  ELSE frpd2.final_raw_min_price_rs_quintal 

              END 

          )::NUMERIC, 2 

      ) AS moving_avg_min_price_rs_quintal, 

      -- Compute moving average for max price 

      ROUND( 

          AVG( 

              CASE  

                  WHEN frpd2.final_raw_max_price_rs_quintal IS NULL  

                       OR frpd2.final_raw_max_price_rs_quintal = 0  

                       OR frpd2.final_raw_max_price_rs_quintal > 100000 THEN NULL 

                  ELSE frpd2.final_raw_max_price_rs_quintal 

              END 

          )::NUMERIC, 2 

      ) AS moving_avg_max_price_rs_quintal, 

      -- Compute moving average for modal price 

      ROUND( 

          AVG( 

              CASE  

                  WHEN frpd2.final_raw_modal_price_rs_quintal IS NULL  

                       OR frpd2.final_raw_modal_price_rs_quintal = 0  

                       OR frpd2.final_raw_modal_price_rs_quintal > 100000 THEN NULL 

                  ELSE frpd2.final_raw_modal_price_rs_quintal 

              END 

          )::NUMERIC, 2 

      ) AS moving_avg_modal_price_rs_quintal 

  FROM final_raw_price_data frpd1 

  LEFT JOIN final_raw_price_data frpd2 

  ON frpd1.symbol = frpd2.symbol 

  AND frpd2.reported_date BETWEEN frpd1.reported_date - INTERVAL '14 days' AND frpd1.reported_date - INTERVAL '1 day' 

  GROUP BY 

      frpd1.reported_date, 

      frpd1.state_name, 

      frpd1.district_name, 

      frpd1.market_name, 

      frpd1.variety, 

      frpd1.symbol, 

      frpd1.uid 

), 

-- Step 4: Define the final_price_for_mdb CTE to calculate final prices based on the conditions 

final_price_for_mdb AS ( 

  SELECT 

      frpd.reported_date, 

      frpd.state_name, 

      frpd.district_name, 

      frpd.market_name, 

      frpd.variety, 

      frpd.symbol, 

      frpd.uid, 

      ma.moving_avg_min_price_rs_quintal, 

      ma.moving_avg_max_price_rs_quintal, 

      ma.moving_avg_modal_price_rs_quintal, 

      -- Final min price with conditions to check deviation from moving average 

      CASE  

          WHEN ABS((frpd.final_raw_min_price_rs_quintal - ma.moving_avg_min_price_rs_quintal) / ma.moving_avg_min_price_rs_quintal) > 0.5  

          THEN ma.moving_avg_min_price_rs_quintal 

          ELSE frpd.final_raw_min_price_rs_quintal 

      END AS final_min_price_rs_quintal, 

      -- Final max price with conditions to check deviation from moving average 

      CASE  

          WHEN ABS((frpd.final_raw_max_price_rs_quintal - ma.moving_avg_max_price_rs_quintal) / ma.moving_avg_max_price_rs_quintal) > 0.5  

          THEN ma.moving_avg_max_price_rs_quintal 

          ELSE frpd.final_raw_max_price_rs_quintal 

      END AS final_max_price_rs_quintal, 

      -- Final modal price with conditions to check deviation from moving average 

      CASE  

          WHEN ABS((frpd.final_raw_modal_price_rs_quintal - ma.moving_avg_modal_price_rs_quintal) / ma.moving_avg_modal_price_rs_quintal) > 0.5  

          THEN ma.moving_avg_modal_price_rs_quintal 

          ELSE frpd.final_raw_modal_price_rs_quintal 

      END AS final_modal_price_rs_quintal 

  FROM final_raw_price_data frpd 

  JOIN moving_avg ma 

  ON frpd.uid||frpd.variety = ma.uid||ma.variety 

) 

 

-- Step 5: Insert the final data into mandi_data.mandi_mdb 

INSERT INTO mandi_data.mandi_mdb ( 

  reported_date,  

  state_name,  

  district_name,  

  market_name,  

  variety,  

  family,  

  jins_name,  

  jins_code,  

  geo_symbol,  

  jins_symbol,  

  symbol,  

  uid,  

  arrivals_tonnes,   

  min_raw, 

  max_raw, 

  modal_raw, 

  min_price_rs_quintal,  

  max_price_rs_quintal,  

  modal_price_rs_quintal, 

  min_price_14_days_moving_avg, 

  max_price_14_days_moving_avg, 

  modal_price_14_days_moving_avg 

) 

SELECT DISTINCT 

  mrm.reported_date,  

  mrm.state_name,  

  mrm.district_name,  

  mrm.market_name,  

  mrm.variety,  

  mrm.family,  

  mrm.jins_name,  

  mrm.jins_code, 

   

 -- Corrected reference to mg and mjs aliases 

  mg.geo_symbol AS geo_symbol,  

  mjs.jins_symbol AS jins_symbol,  

  mg.geo_symbol || mjs.jins_symbol AS symbol,  

  

 -- Generate UID as done in price_data 

  TO_CHAR(mrm.reported_date, 'YYYYMMDD') || mg.geo_symbol || mjs.jins_symbol AS uid, 

 -- Raw values from mrm  

  mrm.min_price_rs_quintal AS min_raw, 

  mrm.max_price_rs_quintal AS max_raw, 

  mrm.modal_price_rs_quintal AS modal_raw, 

   

 -- Arrivals tonnes from mrm 

  mrm.arrivals_tonnes, 

 -- Use final prices from final_price_for_mdb  

  COALESCE(fpmdb.final_min_price_rs_quintal, mrm.min_price_rs_quintal) AS min_price_rs_quintal, 

  COALESCE(fpmdb.final_max_price_rs_quintal, mrm.max_price_rs_quintal) AS max_price_rs_quintal, 

  COALESCE(fpmdb.final_modal_price_rs_quintal, mrm.modal_price_rs_quintal) AS modal_price_rs_quintal, 

 -- Use moving average prices from final_price_for_mdb 

  fpmdb.final_min_price_rs_quintal, 

  fpmdb.final_max_price_rs_quintal, 

  fpmdb.final_modal_price_rs_quintal 

FROM  

  mandi_data.mandi_raw_master mrm 

JOIN  

  mandi_data.mandi_geosymbol mg  

ON  

  mrm.state_name = mg.state_name  

  AND mrm.district_name = mg.district_name  

  AND mrm.market_name = mg.market_name 

JOIN  

  mandi_data.mandi_jinssymbol mjs  

ON  

  mrm.jins_name = mjs.jins_name 

LEFT JOIN  

  final_price_for_mdb fpmdb 

ON  

  TO_CHAR(mrm.reported_date, 'YYYYMMDD') || mg.geo_symbol || mjs.jins_symbol || mrm.variety= fpmdb.uid || fpmdb.variety 

  AND mrm.reported_date = fpmdb.reported_date 

CROSS JOIN 

  global_vars  -- Cross join to use the global year variable 

WHERE 

EXTRACT(YEAR FROM mrm.reported_date) = global_vars.year 