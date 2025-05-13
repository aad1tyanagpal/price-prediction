-- Step 1: Insert data into the exchange_data.commodity_mdb table
INSERT INTO exchange_data.commodity_mdb (
    date,
    commodity_name,
    type,
    exchange,
    ticker,
    symbol,
    unit,
    open,
    close,
    high,
    low,
    average
)
SELECT 
    r.date,  -- Step 2: Select the date from exchange_data.commodity_raw_master
    r.commodity AS commodity_name,  -- Step 3: Map commodity to commodity_name
    d.type,  -- Step 4: Get the type from exchange_data.commodity_details
    d.exchange,  -- Step 5: Get the exchange from exchange_data.commodity_details
    d.ticker,  -- Step 6a: Get the ticker from exchange_data.commodity_details
    d.symbol,	-- Step 6b: Get the symbol from exchange_data.commodity_details
    d.new_unit AS unit,  -- Step 7: Get the new unit from exchange_data.commodity_details

    -- Step 8: Calculate the 'open' price with currency conversion and unit conversion factor if necessary
    CASE 
        WHEN SUBSTRING(r.unit, 1, 3) IN ('USD', 'USc') THEN 
            (r.open * COALESCE(c1.average, 1) * d.unit_conversion_factor)  -- Use USD conversion rate
        WHEN SUBSTRING(r.unit, 1, 3) = 'EUR' THEN 
            (r.open * COALESCE(c2.average, 1) * d.unit_conversion_factor)  -- Use EUR conversion rate
        WHEN SUBSTRING(r.unit, 1, 3) = 'MYR' THEN 
            (r.open * COALESCE(c3.average, 1) * d.unit_conversion_factor)  -- Use MYR conversion rate
        ELSE 
            r.open * d.unit_conversion_factor  -- No conversion needed
    END AS open,

    -- Step 9: Calculate the 'close' price with currency conversion and unit conversion factor if necessary
    CASE 
        WHEN SUBSTRING(r.unit, 1, 3) IN ('USD', 'USc') THEN 
            (r.close * COALESCE(c1.average, 1) * d.unit_conversion_factor)  -- Use USD conversion rate
        WHEN SUBSTRING(r.unit, 1, 3) = 'EUR' THEN 
            (r.close * COALESCE(c2.average, 1) * d.unit_conversion_factor)  -- Use EUR conversion rate
        WHEN SUBSTRING(r.unit, 1, 3) = 'MYR' THEN 
            (r.close * COALESCE(c3.average, 1) * d.unit_conversion_factor)  -- Use MYR conversion rate
        ELSE 
            r.close * d.unit_conversion_factor  -- No conversion needed
    END AS close,

    -- Step 10: Calculate the 'high' price with currency conversion and unit conversion factor if necessary
    CASE 
        WHEN SUBSTRING(r.unit, 1, 3) IN ('USD', 'USc') THEN 
            (r.high * COALESCE(c1.average, 1) * d.unit_conversion_factor)  -- Use USD conversion rate
        WHEN SUBSTRING(r.unit, 1, 3) = 'EUR' THEN 
            (r.high * COALESCE(c2.average, 1) * d.unit_conversion_factor)  -- Use EUR conversion rate
        WHEN SUBSTRING(r.unit, 1, 3) = 'MYR' THEN 
            (r.high * COALESCE(c3.average, 1) * d.unit_conversion_factor)  -- Use MYR conversion rate
        ELSE 
            r.high * d.unit_conversion_factor  -- No conversion needed
    END AS high,

    -- Step 11: Calculate the 'low' price with currency conversion and unit conversion factor if necessary
    CASE 
        WHEN SUBSTRING(r.unit, 1, 3) IN ('USD', 'USc') THEN 
            (r.low * COALESCE(c1.average, 1) * d.unit_conversion_factor)  -- Use USD conversion rate
        WHEN SUBSTRING(r.unit, 1, 3) = 'EUR' THEN 
            (r.low * COALESCE(c2.average, 1) * d.unit_conversion_factor)  -- Use EUR conversion rate
        WHEN SUBSTRING(r.unit, 1, 3) = 'MYR' THEN 
            (r.low * COALESCE(c3.average, 1) * d.unit_conversion_factor)  -- Use MYR conversion rate
        ELSE 
            r.low * d.unit_conversion_factor  -- No conversion needed
    END AS low,

    -- Step 12: Calculate the average of the converted prices with unit conversion factor
    (
        CASE 
            WHEN SUBSTRING(r.unit, 1, 3) IN ('USD', 'USc') THEN 
                ((r.open * COALESCE(c1.average, 1) * d.unit_conversion_factor +
                 r.close * COALESCE(c1.average, 1) * d.unit_conversion_factor +
                 r.high * COALESCE(c1.average, 1) * d.unit_conversion_factor +
                 r.low * COALESCE(c1.average, 1) * d.unit_conversion_factor) / 4)
            WHEN SUBSTRING(r.unit, 1, 3) = 'EUR' THEN 
                ((r.open * COALESCE(c2.average, 1) * d.unit_conversion_factor +
                 r.close * COALESCE(c2.average, 1) * d.unit_conversion_factor +
                 r.high * COALESCE(c2.average, 1) * d.unit_conversion_factor +
                 r.low * COALESCE(c2.average, 1) * d.unit_conversion_factor) / 4)
            WHEN SUBSTRING(r.unit, 1, 3) = 'MYR' THEN 
                ((r.open * COALESCE(c3.average, 1) * d.unit_conversion_factor +
                 r.close * COALESCE(c3.average, 1) * d.unit_conversion_factor +
                 r.high * COALESCE(c3.average, 1) * d.unit_conversion_factor +
                 r.low * COALESCE(c3.average, 1) * d.unit_conversion_factor) / 4)
            ELSE 
                ((r.open * d.unit_conversion_factor +
                 r.close * d.unit_conversion_factor +
                 r.high * d.unit_conversion_factor +
                 r.low * d.unit_conversion_factor) / 4)
        END
    ) AS average

FROM 
    exchange_data.commodity_raw_master r  -- Step 13: Source table for raw commodity data
JOIN 
    exchange_data.commodity_details d  -- Step 14: Join with details for additional metadata
ON 
    r.commodity = d.commodity_name

-- Step 15: Join for USD conversion rates
LEFT JOIN LATERAL 
    (SELECT u.average
     FROM exchange_data.usd_master u
     WHERE u.date <= r.date
     ORDER BY u.date DESC
     LIMIT 1
    ) c1 ON SUBSTRING(r.unit, 1, 3) IN ('USD', 'USc')

-- Step 16: Join for EUR conversion rates
LEFT JOIN LATERAL 
    (SELECT e.average
     FROM exchange_data.eur_master e
     WHERE e.date <= r.date
     ORDER BY e.date DESC
     LIMIT 1
    ) c2 ON SUBSTRING(r.unit, 1, 3) = 'EUR'

-- Step 17: Join for MYR conversion rates
LEFT JOIN LATERAL 
    (SELECT m.average
     FROM exchange_data.myr_master m
     WHERE m.date <= r.date
     ORDER BY m.date DESC
     LIMIT 1
    ) c3 ON SUBSTRING(r.unit, 1, 3) = 'MYR';
