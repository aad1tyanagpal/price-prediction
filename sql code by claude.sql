-- =====================================================
-- AGMARKNET DATABASE SCHEMA WITH AUTOMATED DATA CLEANING
-- Optimized for: Mustard, Wheat, Cluster Beans, Moong, Cotton, Cotton Seed
-- =====================================================

-- Create schemas
CREATE SCHEMA IF NOT EXISTS mandi_data;
CREATE SCHEMA IF NOT EXISTS analytics_data;
CREATE SCHEMA IF NOT EXISTS data_quality;

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- =====================================================
-- REFERENCE TABLES
-- =====================================================

-- States and Districts Reference
CREATE TABLE mandi_data.ref_states (
    state_id SERIAL PRIMARY KEY,
    state_name VARCHAR(100) NOT NULL UNIQUE,
    state_code VARCHAR(10),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE mandi_data.ref_districts (
    district_id SERIAL PRIMARY KEY,
    district_name VARCHAR(100) NOT NULL,
    state_id INTEGER REFERENCES mandi_data.ref_states(state_id),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(district_name, state_id)
);

-- Markets Reference
CREATE TABLE mandi_data.ref_markets (
    market_id SERIAL PRIMARY KEY,
    market_name VARCHAR(200) NOT NULL,
    district_id INTEGER REFERENCES mandi_data.ref_districts(district_id),
    market_code VARCHAR(20),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(market_name, district_id)
);

-- Commodities Reference with price validation rules
CREATE TABLE mandi_data.ref_commodities (
    commodity_id SERIAL PRIMARY KEY,
    commodity_name VARCHAR(100) NOT NULL UNIQUE,
    commodity_code VARCHAR(20),
    unit VARCHAR(20) DEFAULT 'Quintal',
    min_expected_price DECIMAL(10,2) DEFAULT 0,
    max_expected_price DECIMAL(10,2) DEFAULT 100000,
    typical_min_price DECIMAL(10,2),
    typical_max_price DECIMAL(10,2),
    price_volatility_threshold DECIMAL(5,2) DEFAULT 50.0, -- % change threshold
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Insert reference commodity data
INSERT INTO mandi_data.ref_commodities (commodity_name, commodity_code, min_expected_price, max_expected_price, typical_min_price, typical_max_price) VALUES
('Mustard', 'MUST', 3000, 8000, 4000, 6500),
('Wheat', 'WHEAT', 1800, 3500, 2000, 2800),
('Cluster Beans', 'GUAR', 3000, 12000, 4000, 8000),
('Moong', 'MOONG', 4000, 12000, 5000, 9000),
('Cotton', 'COTTON', 4000, 8000, 4500, 7000),
('Cotton Seed', 'CTSEED', 2000, 5000, 2500, 4000);

-- =====================================================
-- MAIN DATA TABLES
-- =====================================================

-- Date dimension table
CREATE TABLE mandi_data.date_dimension (
    date_id SERIAL PRIMARY KEY,
    date_value DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    is_weekend BOOLEAN DEFAULT FALSE,
    is_holiday BOOLEAN DEFAULT FALSE,
    date_processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Raw scraped data table
CREATE TABLE mandi_data.raw_market_data (
    raw_id BIGSERIAL PRIMARY KEY,
    scrape_date DATE NOT NULL,
    market_name VARCHAR(200),
    district_name VARCHAR(100),
    state_name VARCHAR(100),
    commodity_name VARCHAR(100),
    variety VARCHAR(100),
    grade VARCHAR(50),
    min_price DECIMAL(10,2),
    max_price DECIMAL(10,2),
    modal_price DECIMAL(10,2),
    arrival_quantity DECIMAL(12,2),
    arrival_unit VARCHAR(20) DEFAULT 'Quintal',
    price_unit VARCHAR(20) DEFAULT 'Rs/Quintal',
    source_url TEXT,
    raw_data_json JSONB,
    is_processed BOOLEAN DEFAULT FALSE,
    data_quality_score INTEGER DEFAULT 0,
    quality_issues TEXT[],
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Cleaned and validated market data
CREATE TABLE mandi_data.market_data (
    data_id BIGSERIAL PRIMARY KEY,
    reported_date DATE NOT NULL,
    market_id INTEGER REFERENCES mandi_data.ref_markets(market_id),
    commodity_id INTEGER REFERENCES mandi_data.ref_commodities(commodity_id),
    variety VARCHAR(100),
    grade VARCHAR(50),
    min_price DECIMAL(10,2) NOT NULL,
    max_price DECIMAL(10,2) NOT NULL,
    modal_price DECIMAL(10,2) NOT NULL,
    arrival_quantity DECIMAL(12,2),
    price_unit VARCHAR(20) DEFAULT 'Rs/Quintal',
    arrival_unit VARCHAR(20) DEFAULT 'Quintal',
    
    -- Original values before cleaning
    original_min_price DECIMAL(10,2),
    original_max_price DECIMAL(10,2),
    original_modal_price DECIMAL(10,2),
    original_arrival DECIMAL(12,2),
    
    -- Quality indicators
    is_price_corrected BOOLEAN DEFAULT FALSE,
    is_arrival_corrected BOOLEAN DEFAULT FALSE,
    data_quality_score INTEGER DEFAULT 100,
    quality_flags VARCHAR(200)[],
    
    -- Metadata
    raw_data_id BIGINT REFERENCES mandi_data.raw_market_data(raw_id),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    -- Constraints
    CONSTRAINT check_prices CHECK (min_price <= modal_price AND modal_price <= max_price),
    CONSTRAINT check_positive_prices CHECK (min_price > 0 AND max_price > 0 AND modal_price > 0),
    CONSTRAINT check_positive_arrival CHECK (arrival_quantity >= 0),
    UNIQUE(reported_date, market_id, commodity_id, variety, grade)
);

-- =====================================================
-- DATA QUALITY TRACKING
-- =====================================================

-- Data quality log
CREATE TABLE data_quality.quality_log (
    log_id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,
    record_id BIGINT NOT NULL,
    issue_type VARCHAR(50) NOT NULL,
    issue_description TEXT,
    original_value TEXT,
    corrected_value TEXT,
    correction_method VARCHAR(100),
    severity VARCHAR(20) DEFAULT 'MEDIUM', -- LOW, MEDIUM, HIGH, CRITICAL
    is_auto_corrected BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Daily data quality summary
CREATE TABLE data_quality.daily_summary (
    summary_id SERIAL PRIMARY KEY,
    date_processed DATE NOT NULL,
    total_records INTEGER DEFAULT 0,
    valid_records INTEGER DEFAULT 0,
    corrected_records INTEGER DEFAULT 0,
    rejected_records INTEGER DEFAULT 0,
    avg_quality_score DECIMAL(5,2),
    common_issues JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(date_processed)
);

-- =====================================================
-- ANALYTICS TABLES
-- =====================================================

-- Moving averages table (optimized version of your original)
CREATE TABLE analytics_data.moving_averages (
    avg_id BIGSERIAL PRIMARY KEY,
    reported_date DATE NOT NULL,
    market_id INTEGER REFERENCES mandi_data.ref_markets(market_id),
    commodity_id INTEGER REFERENCES mandi_data.ref_commodities(commodity_id),
    variety VARCHAR(100),
    
    -- Current day prices
    current_min_price DECIMAL(10,2),
    current_max_price DECIMAL(10,2),
    current_modal_price DECIMAL(10,2),
    
    -- Past moving averages (min prices)
    past_3d_min_avg DECIMAL(10,2),
    past_5d_min_avg DECIMAL(10,2),
    past_7d_min_avg DECIMAL(10,2),
    past_15d_min_avg DECIMAL(10,2),
    past_30d_min_avg DECIMAL(10,2),
    past_60d_min_avg DECIMAL(10,2),
    past_90d_min_avg DECIMAL(10,2),
    
    -- Future moving averages (min prices)
    future_3d_min_avg DECIMAL(10,2),
    future_5d_min_avg DECIMAL(10,2),
    future_7d_min_avg DECIMAL(10,2),
    future_15d_min_avg DECIMAL(10,2),
    future_30d_min_avg DECIMAL(10,2),
    future_60d_min_avg DECIMAL(10,2),
    future_90d_min_avg DECIMAL(10,2),
    
    -- Past moving averages (max prices)
    past_3d_max_avg DECIMAL(10,2),
    past_5d_max_avg DECIMAL(10,2),
    past_7d_max_avg DECIMAL(10,2),
    past_15d_max_avg DECIMAL(10,2),
    past_30d_max_avg DECIMAL(10,2),
    past_60d_max_avg DECIMAL(10,2),
    past_90d_max_avg DECIMAL(10,2),
    
    -- Future moving averages (max prices)
    future_3d_max_avg DECIMAL(10,2),
    future_5d_max_avg DECIMAL(10,2),
    future_7d_max_avg DECIMAL(10,2),
    future_15d_max_avg DECIMAL(10,2),
    future_30d_max_avg DECIMAL(10,2),
    future_60d_max_avg DECIMAL(10,2),
    future_90d_max_avg DECIMAL(10,2),
    
    calculated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(reported_date, market_id, commodity_id, variety)
);

-- Price trends and volatility
CREATE TABLE analytics_data.price_trends (
    trend_id BIGSERIAL PRIMARY KEY,
    market_id INTEGER REFERENCES mandi_data.ref_markets(market_id),
    commodity_id INTEGER REFERENCES mandi_data.ref_commodities(commodity_id),
    date_from DATE NOT NULL,
    date_to DATE NOT NULL,
    trend_direction VARCHAR(20), -- RISING, FALLING, STABLE, VOLATILE
    price_change_percent DECIMAL(5,2),
    volatility_index DECIMAL(5,2),
    avg_daily_volume DECIMAL(12,2),
    calculated_at TIMESTAMP DEFAULT NOW()
);

-- Market summary view for dashboard
CREATE MATERIALIZED VIEW analytics_data.mv_market_summary AS
SELECT 
    md.reported_date,
    rs.state_name,
    rd.district_name,
    rm.market_name,
    rc.commodity_name,
    COUNT(*) as total_entries,
    AVG(md.modal_price) as avg_modal_price,
    MIN(md.min_price) as lowest_price,
    MAX(md.max_price) as highest_price,
    SUM(md.arrival_quantity) as total_arrival,
    AVG(md.data_quality_score) as avg_quality_score
FROM mandi_data.market_data md
JOIN mandi_data.ref_markets rm ON md.market_id = rm.market_id
JOIN mandi_data.ref_districts rd ON rm.district_id = rd.district_id
JOIN mandi_data.ref_states rs ON rd.state_id = rs.state_id
JOIN mandi_data.ref_commodities rc ON md.commodity_id = rc.commodity_id
WHERE md.reported_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY md.reported_date, rs.state_name, rd.district_name, rm.market_name, rc.commodity_name;

-- Latest prices view
CREATE VIEW analytics_data.v_latest_prices AS
WITH ranked_prices AS (
    SELECT 
        md.*,
        rs.state_name,
        rd.district_name,
        rm.market_name,
        rc.commodity_name,
        ROW_NUMBER() OVER (PARTITION BY md.market_id, md.commodity_id, md.variety ORDER BY md.reported_date DESC) as rn
    FROM mandi_data.market_data md
    JOIN mandi_data.ref_markets rm ON md.market_id = rm.market_id
    JOIN mandi_data.ref_districts rd ON rm.district_id = rd.district_id
    JOIN mandi_data.ref_states rs ON rd.state_id = rs.state_id
    JOIN mandi_data.ref_commodities rc ON md.commodity_id = rc.commodity_id
    WHERE md.data_quality_score >= 70
)
SELECT * FROM ranked_prices WHERE rn = 1;

-- Quality dashboard view
CREATE VIEW data_quality.v_quality_dashboard AS
SELECT 
    DATE_TRUNC('day', created_at) as report_date,
    table_name,
    issue_type,
    COUNT(*) as issue_count,
    COUNT(*) FILTER (WHERE severity = 'CRITICAL') as critical_issues,
    COUNT(*) FILTER (WHERE severity = 'HIGH') as high_issues,
    COUNT(*) FILTER (WHERE is_auto_corrected = true) as auto_corrected
FROM data_quality.quality_log
WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE_TRUNC('day', created_at), table_name, issue_type
ORDER BY report_date DESC, issue_count DESC;



-- =====================================================
-- INDEXES FOR PERFORMANCE
-- =====================================================

-- Raw data indexes
CREATE INDEX idx_raw_market_data_scrape_date ON mandi_data.raw_market_data(scrape_date);
CREATE INDEX idx_raw_market_data_commodity ON mandi_data.raw_market_data(commodity_name);
CREATE INDEX idx_raw_market_data_processed ON mandi_data.raw_market_data(is_processed);
CREATE INDEX idx_raw_market_data_quality ON mandi_data.raw_market_data(data_quality_score);

-- Market data indexes
CREATE INDEX idx_market_data_date ON mandi_data.market_data(reported_date);
CREATE INDEX idx_market_data_market_commodity ON mandi_data.market_data(market_id, commodity_id);
CREATE INDEX idx_market_data_commodity_date ON mandi_data.market_data(commodity_id, reported_date);
CREATE INDEX idx_market_data_quality ON mandi_data.market_data(data_quality_score);
CREATE INDEX idx_market_data_date_market_commodity ON mandi_data.market_data(reported_date, market_id, commodity_id);

-- Composite indexes for analytics
CREATE INDEX idx_market_data_analytics ON mandi_data.market_data(commodity_id, market_id, reported_date) 
    INCLUDE (min_price, max_price, modal_price, arrival_quantity);

-- Moving averages indexes
CREATE INDEX idx_moving_avg_date_commodity ON analytics_data.moving_averages(reported_date, commodity_id);
CREATE INDEX idx_moving_avg_market_commodity ON analytics_data.moving_averages(market_id, commodity_id);

-- Reference table indexes
CREATE INDEX idx_markets_district ON mandi_data.ref_markets(district_id);
CREATE INDEX idx_districts_state ON mandi_data.ref_districts(state_id);
CREATE INDEX idx_commodities_active ON mandi_data.ref_commodities(is_active);

-- Date dimension indexes
CREATE INDEX idx_date_dimension_date ON mandi_data.date_dimension(date_value);
CREATE INDEX idx_date_dimension_year_month ON mandi_data.date_dimension(year, month);
CREATE INDEX idx_date_dimension_processed ON mandi_data.date_dimension(date_processed);

-- Quality log indexes
CREATE INDEX idx_quality_log_table_record ON data_quality.quality_log(table_name, record_id);
CREATE INDEX idx_quality_log_issue_type ON data_quality.quality_log(issue_type);
CREATE INDEX idx_quality_log_created ON data_quality.quality_log(created_at);

-- Additional performance indexes
CREATE INDEX CONCURRENTLY idx_market_data_quality_date ON mandi_data.market_data(data_quality_score, reported_date) 
    WHERE data_quality_score >= 70;

CREATE INDEX CONCURRENTLY idx_raw_data_quality_processing ON mandi_data.raw_market_data(data_quality_score, is_processed);

CREATE INDEX CONCURRENTLY idx_quality_log_severity_date ON data_quality.quality_log(severity, created_at);

-- Unique index for materialized view refresh
CREATE UNIQUE INDEX idx_mv_market_summary_unique ON analytics_data.mv_market_summary(
    reported_date, state_name, district_name, market_name, commodity_name
);

-- =====================================================
-- DATA CLEANING FUNCTIONS
-- =====================================================

-- Function to detect and correct price anomalies
CREATE OR REPLACE FUNCTION mandi_data.clean_price_data(
    p_min_price DECIMAL(10,2),
    p_max_price DECIMAL(10,2),
    p_modal_price DECIMAL(10,2),
    p_commodity_id INTEGER,
    p_market_id INTEGER DEFAULT NULL,
    p_reported_date DATE DEFAULT NULL
) RETURNS TABLE (
    cleaned_min_price DECIMAL(10,2),
    cleaned_max_price DECIMAL(10,2),
    cleaned_modal_price DECIMAL(10,2),
    is_corrected BOOLEAN,
    correction_flags TEXT[]
) AS $$
DECLARE
    v_flags TEXT[] := '{}';
    v_corrected BOOLEAN := FALSE;
    v_temp_price DECIMAL(10,2);
    v_commodity_min DECIMAL(10,2);
    v_commodity_max DECIMAL(10,2);
    v_historical_avg DECIMAL(10,2);
BEGIN
    -- Get commodity price bounds
    SELECT min_expected_price, max_expected_price 
    INTO v_commodity_min, v_commodity_max
    FROM mandi_data.ref_commodities 
    WHERE commodity_id = p_commodity_id;
    
    -- Initialize with input values
    cleaned_min_price := p_min_price;
    cleaned_max_price := p_max_price;
    cleaned_modal_price := p_modal_price;
    
    -- Rule 1: Check if prices are swapped (min > max)
    IF p_min_price > p_max_price THEN
        cleaned_min_price := p_max_price;
        cleaned_max_price := p_min_price;
        v_flags := array_append(v_flags, 'MIN_MAX_SWAPPED');
        v_corrected := TRUE;
    END IF;
    
    -- Rule 2: Check if modal price is outside min-max range
    IF p_modal_price < cleaned_min_price OR p_modal_price > cleaned_max_price THEN
        -- Set modal to average of min and max
        cleaned_modal_price := (cleaned_min_price + cleaned_max_price) / 2;
        v_flags := array_append(v_flags, 'MODAL_PRICE_CORRECTED');
        v_corrected := TRUE;
    END IF;
    
    -- Rule 3: Check for extremely high prices
    IF cleaned_max_price > v_commodity_max THEN
        -- Get historical average for context
        SELECT AVG(max_price) INTO v_historical_avg
        FROM mandi_data.market_data md
        WHERE md.commodity_id = p_commodity_id 
          AND (p_market_id IS NULL OR md.market_id = p_market_id)
          AND md.reported_date >= COALESCE(p_reported_date, CURRENT_DATE) - INTERVAL '30 days'
          AND md.reported_date < COALESCE(p_reported_date, CURRENT_DATE);
        
        -- If historical average exists and current price is too high, cap it
        IF v_historical_avg IS NOT NULL AND cleaned_max_price > v_historical_avg * 2 THEN
            cleaned_max_price := LEAST(v_historical_avg * 1.5, v_commodity_max);
            cleaned_modal_price := LEAST(cleaned_modal_price, cleaned_max_price);
            cleaned_min_price := LEAST(cleaned_min_price, cleaned_modal_price);
            v_flags := array_append(v_flags, 'HIGH_PRICE_CAPPED');
            v_corrected := TRUE;
        END IF;
    END IF;
    
    -- Rule 4: Check for extremely low prices
    IF cleaned_min_price < v_commodity_min THEN
        cleaned_min_price := v_commodity_min;
        cleaned_modal_price := GREATEST(cleaned_modal_price, cleaned_min_price);
        cleaned_max_price := GREATEST(cleaned_max_price, cleaned_modal_price);
        v_flags := array_append(v_flags, 'LOW_PRICE_ADJUSTED');
        v_corrected := TRUE;
    END IF;
    
    -- Rule 5: Ensure logical price progression
    IF cleaned_min_price > cleaned_modal_price THEN
        cleaned_modal_price := (cleaned_min_price + cleaned_max_price) / 2;
        v_flags := array_append(v_flags, 'PRICE_SEQUENCE_FIXED');
        v_corrected := TRUE;
    END IF;
    
    is_corrected := v_corrected;
    correction_flags := v_flags;
    
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- Function to validate arrival quantities
CREATE OR REPLACE FUNCTION mandi_data.clean_arrival_data(
    p_arrival DECIMAL(12,2),
    p_commodity_id INTEGER,
    p_market_id INTEGER DEFAULT NULL
) RETURNS TABLE (
    cleaned_arrival DECIMAL(12,2),
    is_corrected BOOLEAN,
    correction_note TEXT
) AS $$
DECLARE
    v_avg_arrival DECIMAL(12,2);
    v_max_reasonable DECIMAL(12,2);
BEGIN
    -- Initialize
    cleaned_arrival := p_arrival;
    is_corrected := FALSE;
    correction_note := NULL;
    
    -- Handle negative or zero arrivals
    IF p_arrival <= 0 THEN
        cleaned_arrival := NULL;
        is_corrected := TRUE;
        correction_note := 'Negative or zero arrival set to NULL';
        RETURN NEXT;
        RETURN;
    END IF;
    
    -- Get historical average arrival for the commodity/market
    SELECT AVG(arrival_quantity) INTO v_avg_arrival
    FROM mandi_data.market_data
    WHERE commodity_id = p_commodity_id
      AND (p_market_id IS NULL OR market_id = p_market_id)
      AND arrival_quantity > 0
      AND reported_date >= CURRENT_DATE - INTERVAL '90 days';
    
    -- If we have historical data, check for anomalies
    IF v_avg_arrival IS NOT NULL AND v_avg_arrival > 0 THEN
        v_max_reasonable := v_avg_arrival * 10; -- 10x average seems reasonable as max
        
        IF p_arrival > v_max_reasonable THEN
            cleaned_arrival := v_max_reasonable;
            is_corrected := TRUE;
            correction_note := 'Extremely high arrival capped to 10x historical average';
        END IF;
    END IF;
    
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- =====================================================
-- TRIGGERS FOR AUTOMATED DATA CLEANING
-- =====================================================

-- Trigger function for raw data processing
CREATE OR REPLACE FUNCTION mandi_data.process_raw_data()
RETURNS TRIGGER AS $$
DECLARE
    v_market_id INTEGER;
    v_commodity_id INTEGER;
    v_state_id INTEGER;
    v_district_id INTEGER;
    v_quality_score INTEGER := 100;
    v_quality_issues TEXT[] := '{}';
    v_price_result RECORD;
    v_arrival_result RECORD;
BEGIN
    -- Skip if already processed
    IF NEW.is_processed THEN
        RETURN NEW;
    END IF;
    
    -- Basic data validation
    IF NEW.commodity_name IS NULL OR NEW.market_name IS NULL THEN
        NEW.data_quality_score := 0;
        NEW.quality_issues := array_append(NEW.quality_issues, 'MISSING_REQUIRED_FIELDS');
        RETURN NEW;
    END IF;
    
    -- Find or create state
    INSERT INTO mandi_data.ref_states (state_name)
    VALUES (NEW.state_name)
    ON CONFLICT (state_name) DO NOTHING;
    
    SELECT state_id INTO v_state_id
    FROM mandi_data.ref_states
    WHERE state_name = NEW.state_name;
    
    -- Find or create district
    INSERT INTO mandi_data.ref_districts (district_name, state_id)
    VALUES (NEW.district_name, v_state_id)
    ON CONFLICT (district_name, state_id) DO NOTHING;
    
    SELECT district_id INTO v_district_id
    FROM mandi_data.ref_districts
    WHERE district_name = NEW.district_name AND state_id = v_state_id;
    
    -- Find or create market
    INSERT INTO mandi_data.ref_markets (market_name, district_id)
    VALUES (NEW.market_name, v_district_id)
    ON CONFLICT (market_name, district_id) DO NOTHING;
    
    SELECT market_id INTO v_market_id
    FROM mandi_data.ref_markets rm
    JOIN mandi_data.ref_districts rd ON rm.district_id = rd.district_id
    WHERE rm.market_name = NEW.market_name AND rd.district_name = NEW.district_name;
    
    -- Find commodity
    SELECT commodity_id INTO v_commodity_id
    FROM mandi_data.ref_commodities
    WHERE LOWER(commodity_name) = LOWER(NEW.commodity_name)
       OR LOWER(commodity_name) LIKE '%' || LOWER(NEW.commodity_name) || '%';
    
    -- If commodity not found, skip processing
    IF v_commodity_id IS NULL THEN
        NEW.data_quality_score := 20;
        NEW.quality_issues := array_append(NEW.quality_issues, 'UNKNOWN_COMMODITY');
        RETURN NEW;
    END IF;
    
    -- Validate and clean price data
    IF NEW.min_price IS NOT NULL AND NEW.max_price IS NOT NULL AND NEW.modal_price IS NOT NULL THEN
        SELECT * INTO v_price_result
        FROM mandi_data.clean_price_data(
            NEW.min_price, NEW.max_price, NEW.modal_price,
            v_commodity_id, v_market_id, NEW.scrape_date
        );
        
        IF v_price_result.is_corrected THEN
            v_quality_score := v_quality_score - 15;
            v_quality_issues := v_quality_issues || v_price_result.correction_flags;
        END IF;
    ELSE
        v_quality_score := v_quality_score - 30;
        v_quality_issues := array_append(v_quality_issues, 'MISSING_PRICE_DATA');
    END IF;
    
    -- Validate arrival data
    IF NEW.arrival_quantity IS NOT NULL THEN
        SELECT * INTO v_arrival_result
        FROM mandi_data.clean_arrival_data(NEW.arrival_quantity, v_commodity_id, v_market_id);
        
        IF v_arrival_result.is_corrected THEN
            v_quality_score := v_quality_score - 10;
            v_quality_issues := array_append(v_quality_issues, 'ARRIVAL_CORRECTED');
        END IF;
    END IF;
    
    -- Update quality metrics
    NEW.data_quality_score := v_quality_score;
    NEW.quality_issues := v_quality_issues;
    NEW.updated_at := NOW();
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for raw data processing
CREATE TRIGGER trigger_process_raw_data
    BEFORE INSERT OR UPDATE ON mandi_data.raw_market_data
    FOR EACH ROW
    EXECUTE FUNCTION mandi_data.process_raw_data();

-- Trigger function for inserting cleaned data
CREATE OR REPLACE FUNCTION mandi_data.insert_cleaned_data()
RETURNS TRIGGER AS $$
DECLARE
    v_market_id INTEGER;
    v_commodity_id INTEGER;
    v_price_result RECORD;
    v_arrival_result RECORD;
BEGIN
    -- Only process high-quality records
    IF NEW.data_quality_score < 50 OR NEW.is_processed THEN
        RETURN NEW;
    END IF;
    
    -- Get market and commodity IDs
    SELECT rm.market_id INTO v_market_id
    FROM mandi_data.ref_markets rm
    JOIN mandi_data.ref_districts rd ON rm.district_id = rd.district_id
    JOIN mandi_data.ref_states rs ON rd.state_id = rs.state_id
    WHERE rm.market_name = NEW.market_name;
    
    SELECT commodity_id INTO v_commodity_id
    FROM mandi_data.ref_commodities
    WHERE LOWER(commodity_name) = LOWER(NEW.commodity_name);
    
    IF v_market_id IS NULL OR v_commodity_id IS NULL THEN
        RETURN NEW;
    END IF;
    
    -- Clean the data
    SELECT * INTO v_price_result
    FROM mandi_data.clean_price_data(
        NEW.min_price, NEW.max_price, NEW.modal_price,
        v_commodity_id, v_market_id, NEW.scrape_date
    );
    
    SELECT * INTO v_arrival_result
    FROM mandi_data.clean_arrival_data(NEW.arrival_quantity, v_commodity_id, v_market_id);
    
    -- Insert into cleaned data table
    INSERT INTO mandi_data.market_data (
        reported_date, market_id, commodity_id, variety, grade,
        min_price, max_price, modal_price, arrival_quantity,
        original_min_price, original_max_price, original_modal_price, original_arrival,
        is_price_corrected, is_arrival_corrected,
        data_quality_score, quality_flags, raw_data_id
    ) VALUES (
        NEW.scrape_date, v_market_id, v_commodity_id, NEW.variety, NEW.grade,
        v_price_result.cleaned_min_price, v_price_result.cleaned_max_price, v_price_result.cleaned_modal_price,
        v_arrival_result.cleaned_arrival,
        NEW.min_price, NEW.max_price, NEW.modal_price, NEW.arrival_quantity,
        v_price_result.is_corrected, v_arrival_result.is_corrected,
        NEW.data_quality_score, NEW.quality_issues, NEW.raw_id
    ) ON CONFLICT (reported_date, market_id, commodity_id, variety, grade) 
      DO UPDATE SET
        min_price = EXCLUDED.min_price,
        max_price = EXCLUDED.max_price,
        modal_price = EXCLUDED.modal_price,
        arrival_quantity = EXCLUDED.arrival_quantity,
        updated_at = NOW();
    
    -- Mark as processed
    NEW.is_processed := TRUE;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for cleaned data insertion
CREATE TRIGGER trigger_insert_cleaned_data
    AFTER UPDATE ON mandi_data.raw_market_data
    FOR EACH ROW
    WHEN (NEW.data_quality_score >= 50 AND NOT OLD.is_processed AND NEW.is_processed IS DISTINCT FROM OLD.is_processed)
    EXECUTE FUNCTION mandi_data.insert_cleaned_data();

-- =====================================================
-- UTILITY FUNCTIONS
-- =====================================================

-- Function to populate date dimension
CREATE OR REPLACE FUNCTION mandi_data.populate_date_dimension(
    start_date DATE DEFAULT '2020-01-01',
    end_date DATE DEFAULT '2030-12-31'
) RETURNS INTEGER AS $$
DECLARE
    current_date DATE := start_date;
    inserted_count INTEGER := 0;
BEGIN
    WHILE current_date <= end_date LOOP
        INSERT INTO mandi_data.date_dimension (
            date_value, year, month, day, quarter, day_of_week,
            day_name, month_name, is_weekend
        ) VALUES (
            current_date,
            EXTRACT(YEAR FROM current_date),
            EXTRACT(MONTH FROM current_date),
            EXTRACT(DAY FROM current_date),
            EXTRACT(QUARTER FROM current_date),
            EXTRACT(DOW FROM current_date),
            TO_CHAR(current_date, 'Day'),
            TO_CHAR(current_date, 'Month'),
            EXTRACT(DOW FROM current_date) IN (0, 6)
        ) ON CONFLICT (date_value) DO NOTHING;
        
        IF FOUND THEN
            inserted_count := inserted_count + 1;
        END IF;
        
        current_date := current_date + INTERVAL '1 day';
    END LOOP;
    
    RETURN inserted_count;
END;
$$ LANGUAGE plpgsql;

-- Function to calculate moving averages (optimized version)
CREATE OR REPLACE FUNCTION analytics_data.calculate_moving_averages(
    p_commodity_id INTEGER DEFAULT NULL,
    p_market_id INTEGER DEFAULT NULL,
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER := 0;
BEGIN
    -- Set default date range if not provided
    p_start_date := COALESCE(p_start_date, CURRENT_DATE - INTERVAL '1 year');
    p_end_date := COALESCE(p_end_date, CURRENT_DATE);
    
    -- Delete existing records for the date range
    DELETE FROM analytics_data.moving_averages
    WHERE reported_date BETWEEN p_start_date AND p_end_date
      AND (p_commodity_id IS NULL OR commodity_id = p_commodity_id)
      AND (p_market_id IS NULL OR market_id = p_market_id);
    
    -- Calculate and insert moving averages using window functions
    WITH price_data AS (
        SELECT 
            md.reported_date,
            md.market_id,
            md.commodity_id,
            md.variety,
            md.min_price,
            md.max_price,
            md.modal_price,
            -- Past moving averages for min prices
            AVG(md.min_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS past_3d_min_avg,
            AVG(md.min_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS past_5d_min_avg,
            AVG(md.min_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ) AS past_7d_min_avg,
            AVG(md.min_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING
            ) AS past_15d_min_avg,
            AVG(md.min_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
            ) AS past_30d_min_avg,
            AVG(md.min_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
            ) AS past_60d_min_avg,
            AVG(md.min_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 90 PRECEDING AND 1 PRECEDING
            ) AS past_90d_min_avg,
            -- Future moving averages for min prices
            AVG(md.min_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING
            ) AS future_3d_min_avg,
            AVG(md.min_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING
            ) AS future_5d_min_avg,
            AVG(md.min_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 1 FOLLOWING AND 7 FOLLOWING
            ) AS future_7d_min_avg,
            AVG(md.min_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING
            ) AS future_15d_min_avg,
            AVG(md.min_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 1 FOLLOWING AND 30 FOLLOWING
            ) AS future_30d_min_avg,
            AVG(md.min_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 1 FOLLOWING AND 60 FOLLOWING
            ) AS future_60d_min_avg,
            AVG(md.min_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 1 FOLLOWING AND 90 FOLLOWING
            ) AS future_90d_min_avg,
            -- Past moving averages for max prices
            AVG(md.max_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
            ) AS past_3d_max_avg,
            AVG(md.max_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING
            ) AS past_5d_max_avg,
            AVG(md.max_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ) AS past_7d_max_avg,
            AVG(md.max_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING
            ) AS past_15d_max_avg,
            AVG(md.max_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
            ) AS past_30d_max_avg,
            AVG(md.max_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
            ) AS past_60d_max_avg,
            AVG(md.max_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 90 PRECEDING AND 1 PRECEDING
            ) AS past_90d_max_avg,
            -- Future moving averages for max prices
            AVG(md.max_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING
            ) AS future_3d_max_avg,
            AVG(md.max_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 1 FOLLOWING AND 5 FOLLOWING
            ) AS future_5d_max_avg,
            AVG(md.max_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 1 FOLLOWING AND 7 FOLLOWING
            ) AS future_7d_max_avg,
            AVG(md.max_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 1 FOLLOWING AND 15 FOLLOWING
            ) AS future_15d_max_avg,
            AVG(md.max_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 1 FOLLOWING AND 30 FOLLOWING
            ) AS future_30d_max_avg,
            AVG(md.max_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 1 FOLLOWING AND 60 FOLLOWING
            ) AS future_60d_max_avg,
            AVG(md.max_price) OVER (
                PARTITION BY md.market_id, md.commodity_id, md.variety 
                ORDER BY md.reported_date 
                ROWS BETWEEN 1 FOLLOWING AND 90 FOLLOWING
            ) AS future_90d_max_avg
        FROM mandi_data.market_data md
        WHERE md.reported_date BETWEEN p_start_date - INTERVAL '90 days' AND p_end_date + INTERVAL '90 days'
          AND (p_commodity_id IS NULL OR md.commodity_id = p_commodity_id)
          AND (p_market_id IS NULL OR md.market_id = p_market_id)
          AND md.data_quality_score >= 70
    )
    INSERT INTO analytics_data.moving_averages (
        reported_date, market_id, commodity_id, variety,
        current_min_price, current_max_price, current_modal_price,
        past_3d_min_avg, past_5d_min_avg, past_7d_min_avg, past_15d_min_avg,
        past_30d_min_avg, past_60d_min_avg, past_90d_min_avg,
        future_3d_min_avg, future_5d_min_avg, future_7d_min_avg, future_15d_min_avg,
        future_30d_min_avg, future_60d_min_avg, future_90d_min_avg,
        past_3d_max_avg, past_5d_max_avg, past_7d_max_avg, past_15d_max_avg,
        past_30d_max_avg, past_60d_max_avg, past_90d_max_avg,
        future_3d_max_avg, future_5d_max_avg, future_7d_max_avg, future_15d_max_avg,
        future_30d_max_avg, future_60d_max_avg, future_90d_max_avg
    )
    SELECT 
        reported_date, market_id, commodity_id, variety,
        min_price, max_price, modal_price,
        past_3d_min_avg, past_5d_min_avg, past_7d_min_avg, past_15d_min_avg,
        past_30d_min_avg, past_60d_min_avg, past_90d_min_avg,
        future_3d_min_avg, future_5d_min_avg, future_7d_min_avg, future_15d_min_avg,
        future_30d_min_avg, future_60d_min_avg, future_90d_min_avg,
        past_3d_max_avg, past_5d_max_avg, past_7d_max_avg, past_15d_max_avg,
        past_30d_max_avg, past_60d_max_avg, past_90d_max_avg,
        future_3d_max_avg, future_5d_max_avg, future_7d_max_avg, future_15d_max_avg,
        future_30d_max_avg, future_60d_max_avg, future_90d_max_avg
    FROM price_data
    WHERE reported_date BETWEEN p_start_date AND p_end_date;
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- Function to populate daily quality summary
CREATE OR REPLACE FUNCTION data_quality.update_daily_summary(
    p_date DATE DEFAULT CURRENT_DATE
) RETURNS VOID AS $$
DECLARE
    v_total_records INTEGER;
    v_valid_records INTEGER;
    v_corrected_records INTEGER;
    v_rejected_records INTEGER;
    v_avg_quality DECIMAL(5,2);
    v_common_issues JSONB;
BEGIN
    -- Get daily statistics
    SELECT 
        COUNT(*),
        COUNT(*) FILTER (WHERE data_quality_score >= 70),
        COUNT(*) FILTER (WHERE array_length(quality_issues, 1) > 0),
        COUNT(*) FILTER (WHERE data_quality_score < 50),
        AVG(data_quality_score)
    INTO v_total_records, v_valid_records, v_corrected_records, v_rejected_records, v_avg_quality
    FROM mandi_data.raw_market_data
    WHERE scrape_date = p_date;
    
    -- Get common issues as JSON
    SELECT jsonb_object_agg(issue, count) INTO v_common_issues
    FROM (
        SELECT 
            unnest(quality_issues) as issue,
            COUNT(*) as count
        FROM mandi_data.raw_market_data
        WHERE scrape_date = p_date
        GROUP BY unnest(quality_issues)
        ORDER BY count DESC
        LIMIT 10
    ) issues;
    
    -- Insert or update summary
    INSERT INTO data_quality.daily_summary (
        date_processed, total_records, valid_records, corrected_records,
        rejected_records, avg_quality_score, common_issues
    ) VALUES (
        p_date, v_total_records, v_valid_records, v_corrected_records,
        v_rejected_records, v_avg_quality, v_common_issues
    ) ON CONFLICT (date_processed) DO UPDATE SET
        total_records = EXCLUDED.total_records,
        valid_records = EXCLUDED.valid_records,
        corrected_records = EXCLUDED.corrected_records,
        rejected_records = EXCLUDED.rejected_records,
        avg_quality_score = EXCLUDED.avg_quality_score,
        common_issues = EXCLUDED.common_issues,
        created_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- Function to calculate price trends
CREATE OR REPLACE FUNCTION analytics_data.calculate_price_trends(
    p_days INTEGER DEFAULT 30,
    p_commodity_id INTEGER DEFAULT NULL
) RETURNS INTEGER AS $$
DECLARE
    v_count INTEGER := 0;
BEGIN
    -- Clear existing trends for the period
    DELETE FROM analytics_data.price_trends
    WHERE date_to >= CURRENT_DATE - p_days
      AND (p_commodity_id IS NULL OR commodity_id = p_commodity_id);
    
    -- Calculate trends
    INSERT INTO analytics_data.price_trends (
        market_id, commodity_id, date_from, date_to,
        trend_direction, price_change_percent, volatility_index, avg_daily_volume
    )
    WITH price_stats AS (
        SELECT 
            market_id,
            commodity_id,
            MIN(reported_date) as date_from,
            MAX(reported_date) as date_to,
            FIRST_VALUE(modal_price) OVER (PARTITION BY market_id, commodity_id ORDER BY reported_date) as first_price,
            LAST_VALUE(modal_price) OVER (PARTITION BY market_id, commodity_id ORDER BY reported_date 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_price,
            AVG(modal_price) as avg_price,
            STDDEV(modal_price) as price_stddev,
            AVG(arrival_quantity) as avg_volume,
            COUNT(*) as data_points
        FROM mandi_data.market_data
        WHERE reported_date >= CURRENT_DATE - p_days
          AND (p_commodity_id IS NULL OR commodity_id = p_commodity_id)
          AND data_quality_score >= 70
        GROUP BY market_id, commodity_id
        HAVING COUNT(*) >= 5  -- At least 5 data points
    )
    SELECT 
        market_id,
        commodity_id,
        date_from,
        date_to,
        CASE 
            WHEN ((last_price - first_price) / first_price * 100) > 10 THEN 'RISING'
            WHEN ((last_price - first_price) / first_price * 100) < -10 THEN 'FALLING'
            WHEN (price_stddev / avg_price * 100) > 20 THEN 'VOLATILE'
            ELSE 'STABLE'
        END as trend_direction,
        ROUND(((last_price - first_price) / first_price * 100)::numeric, 2) as price_change_percent,
        ROUND((price_stddev / avg_price * 100)::numeric, 2) as volatility_index,
        ROUND(avg_volume::numeric, 2) as avg_daily_volume
    FROM price_stats;
    
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END;
$$ LANGUAGE plpgsql;

-- Function for bulk data import with validation
CREATE OR REPLACE FUNCTION mandi_data.bulk_import_raw_data(
    p_batch_size INTEGER DEFAULT 1000
) RETURNS TABLE (
    processed_count INTEGER,
    success_count INTEGER,
    error_count INTEGER,
    avg_quality_score DECIMAL(5,2)
) AS $$
DECLARE
    v_processed INTEGER := 0;
    v_success INTEGER := 0;
    v_error INTEGER := 0;
    v_avg_quality DECIMAL(5,2);
    batch_record RECORD;
BEGIN
    -- Process unprocessed records in batches
    FOR batch_record IN
        WITH batched_data AS (
            SELECT *, ROW_NUMBER() OVER (ORDER BY raw_id) as batch_num
            FROM mandi_data.raw_market_data
            WHERE is_processed = FALSE
            LIMIT p_batch_size
        )
        SELECT * FROM batched_data
    LOOP
        BEGIN
            -- Update record to trigger processing
            UPDATE mandi_data.raw_market_data
            SET updated_at = NOW()
            WHERE raw_id = batch_record.raw_id;
            
            v_processed := v_processed + 1;
            
            -- Check if processing was successful
            IF batch_record.data_quality_score >= 50 THEN
                v_success := v_success + 1;
            ELSE
                v_error := v_error + 1;
            END IF;
            
        EXCEPTION WHEN OTHERS THEN
            v_error := v_error + 1;
            
            -- Log the error
            INSERT INTO data_quality.quality_log (
                table_name, record_id, issue_type, issue_description, severity
            ) VALUES (
                'raw_market_data', batch_record.raw_id, 'PROCESSING_ERROR', 
                SQLERRM, 'HIGH'
            );
        END;
    END LOOP;
    
    -- Calculate average quality score
    SELECT AVG(data_quality_score) INTO v_avg_quality
    FROM mandi_data.raw_market_data
    WHERE is_processed = TRUE
      AND updated_at >= NOW() - INTERVAL '1 hour';
    
    processed_count := v_processed;
    success_count := v_success;
    error_count := v_error;
    avg_quality_score := COALESCE(v_avg_quality, 0);
    
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;

-- Data archiving function
CREATE OR REPLACE FUNCTION mandi_data.archive_old_data(
    p_archive_before_days INTEGER DEFAULT 2555 -- ~7 years
) RETURNS INTEGER AS $$
DECLARE
    v_archived_count INTEGER := 0;
    v_cutoff_date DATE;
BEGIN
    v_cutoff_date := CURRENT_DATE - p_archive_before_days;
    
    -- Create archive table if not exists
    CREATE TABLE IF NOT EXISTS mandi_data.market_data_archive (
        LIKE mandi_data.market_data INCLUDING ALL
    );
    
    -- Move old data to archive
    WITH archived_data AS (
        DELETE FROM mandi_data.market_data
        WHERE reported_date < v_cutoff_date
        RETURNING *
    )
    INSERT INTO mandi_data.market_data_archive
    SELECT * FROM archived_data;
    
    GET DIAGNOSTICS v_archived_count = ROW_COUNT;
    
    -- Clean up old raw data
    DELETE FROM mandi_data.raw_market_data
    WHERE scrape_date < v_cutoff_date
      AND is_processed = TRUE;
    
    -- Clean up old quality logs
    DELETE FROM data_quality.quality_log
    WHERE created_at < v_cutoff_date;
    
    RETURN v_archived_count;
END;
$$ LANGUAGE plpgsql;

-- Function to refresh materialized views
CREATE OR REPLACE FUNCTION analytics_data.refresh_materialized_views()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW analytics_data.mv_market_summary;
    
    -- Log the refresh
    INSERT INTO data_quality.quality_log (
        table_name, record_id, issue_type, issue_description, severity
    ) VALUES (
        'mv_market_summary', 0, 'REFRESH', 
        'Materialized view refreshed successfully', 'LOW'
    );
END;
$$ LANGUAGE plpgsql;

-- Automated maintenance procedures
CREATE OR REPLACE FUNCTION mandi_data.daily_maintenance()
RETURNS TEXT AS $$
DECLARE
    v_result TEXT := '';
    v_temp_count INTEGER;
BEGIN
    -- Update daily quality summary
    PERFORM data_quality.update_daily_summary();
    v_result := v_result || 'Quality summary updated. ';
    
    -- Calculate price trends
    SELECT analytics_data.calculate_price_trends(30) INTO v_temp_count;
    v_result := v_result || 'Price trends calculated (' || v_temp_count || ' records). ';
    
    -- Refresh materialized views
    PERFORM analytics_data.refresh_materialized_views();
    v_result := v_result || 'Materialized views refreshed. ';
    
    -- Update table statistics
    ANALYZE mandi_data.market_data;
    ANALYZE mandi_data.raw_market_data;
    v_result := v_result || 'Table statistics updated. ';
    
    -- Archive old data (run monthly)
    IF EXTRACT(DAY FROM CURRENT_DATE) = 1 THEN
        SELECT mandi_data.archive_old_data() INTO v_temp_count;
        v_result := v_result || 'Archived ' || v_temp_count || ' old records. ';
    END IF;
    
    RETURN v_result;
END;
$$ LANGUAGE plpgsql;

-- Security roles and permissions
CREATE ROLE IF NOT EXISTS mandi_readonly;
CREATE ROLE IF NOT EXISTS mandi_analyst;
CREATE ROLE IF NOT EXISTS mandi_admin;

-- Grant permissions
GRANT USAGE ON SCHEMA mandi_data TO mandi_readonly, mandi_analyst, mandi_admin;
GRANT USAGE ON SCHEMA analytics_data TO mandi_readonly, mandi_analyst, mandi_admin;
GRANT USAGE ON SCHEMA data_quality TO mandi_analyst, mandi_admin;

GRANT SELECT ON ALL TABLES IN SCHEMA mandi_data TO mandi_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics_data TO mandi_readonly;

GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA mandi_data TO mandi_analyst;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA analytics_data TO mandi_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA data_quality TO mandi_analyst;

GRANT ALL ON ALL TABLES IN SCHEMA mandi_data TO mandi_admin;
GRANT ALL ON ALL TABLES IN SCHEMA analytics_data TO mandi_admin;
GRANT ALL ON ALL TABLES IN SCHEMA data_quality TO mandi_admin;

-- Grant sequence permissions
GRANT USAGE ON ALL SEQUENCES IN SCHEMA mandi_data TO mandi_analyst, mandi_admin;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA analytics_data TO mandi_analyst, mandi_admin;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA data_quality TO mandi_admin;

-- Setup automated daily maintenance (PostgreSQL cron extension required)
-- SELECT cron.schedule('daily-maintenance', '0 2 * * *', 'SELECT mandi_data.daily_maintenance();');

-- Initial data population
SELECT mandi_data.populate_date_dimension();

-- Create alert function for critical data quality issues
CREATE OR REPLACE FUNCTION data_quality.check_critical_issues()
RETURNS TEXT AS $$
DECLARE
    v_critical_count INTEGER;
    v_low_quality_count INTEGER;
    v_result TEXT := '';
BEGIN
    -- Check for critical quality issues in last 24 hours
    SELECT COUNT(*) INTO v_critical_count
    FROM data_quality.quality_log
    WHERE severity = 'CRITICAL'
      AND created_at >= NOW() - INTERVAL '24 hours';
    
    -- Check for high number of low quality records today
    SELECT COUNT(*) INTO v_low_quality_count
    FROM mandi_data.raw_market_data
    WHERE scrape_date = CURRENT_DATE
      AND data_quality_score < 50;
    
    IF v_critical_count > 0 THEN
        v_result := v_result || 'ALERT: ' || v_critical_count || ' critical data quality issues found. ';
    END IF;
    
    IF v_low_quality_count > 100 THEN
        v_result := v_result || 'WARNING: ' || v_low_quality_count || ' low quality records today. ';
    END IF;
    
    IF v_result = '' THEN
        v_result := 'No critical issues detected.';
    END IF;
    
    RETURN v_result;
END;
$$ LANGUAGE plpgsql;