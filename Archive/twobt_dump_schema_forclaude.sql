PGDMP      0                }            2bt_database    17.0    17.0     V           0    0    ENCODING    ENCODING        SET client_encoding = 'UTF8';
                           false            W           0    0 
   STDSTRINGS 
   STDSTRINGS     (   SET standard_conforming_strings = 'on';
                           false            X           0    0 
   SEARCHPATH 
   SEARCHPATH     8   SELECT pg_catalog.set_config('search_path', '', false);
                           false            Y           1262    16399    2bt_database    DATABASE     �   CREATE DATABASE "2bt_database" WITH TEMPLATE = template0 ENCODING = 'UTF8' LOCALE_PROVIDER = libc LOCALE = 'English_India.1252';
    DROP DATABASE "2bt_database";
                     aad1tyanagpal    false                        2615    16416    analytics_data    SCHEMA        CREATE SCHEMA analytics_data;
    DROP SCHEMA analytics_data;
                     aad1tyanagpal    false                        2615    16417    exchange_data    SCHEMA        CREATE SCHEMA exchange_data;
    DROP SCHEMA exchange_data;
                     postgres    false                        2615    16418 
   mandi_data    SCHEMA        CREATE SCHEMA mandi_data;
    DROP SCHEMA mandi_data;
                     aad1tyanagpal    false            Z           0    0    SCHEMA mandi_data    COMMENT     :   COMMENT ON SCHEMA mandi_data IS 'standard public schema';
                        aad1tyanagpal    false    8            [           0    0    SCHEMA mandi_data    ACL     ,   GRANT USAGE ON SCHEMA mandi_data TO PUBLIC;
                        aad1tyanagpal    false    8                       1259    16490    model0_reference_curve    TABLE       CREATE TABLE analytics_data.model0_reference_curve (
    time_stamp timestamp without time zone,
    reported_date date,
    uid character varying(255) NOT NULL,
    market_name character varying(255),
    geo_symbol character varying(255),
    jins_name character varying(255),
    jins_symbol character varying(255),
    max_price_rs_quintal numeric(10,2),
    max_raw numeric(10,2),
    equation text,
    days_since_start integer,
    ideal_max numeric(10,2),
    max_to_ideal_max_ratio numeric(10,4),
    year character varying(10)
);
 2   DROP TABLE analytics_data.model0_reference_curve;
       analytics_data         heap r       postgres    false    6                       1259    16497    model1_jins_market_moving_avg    TABLE     u
  CREATE TABLE analytics_data.model1_jins_market_moving_avg (
    reported_date date,
    market_name character varying(255),
    jins_name character varying(255),
    symbol character varying(50),
    date_time_stamp timestamp without time zone,
    min_price_from_mdb numeric(10,2),
    raw_max_price_from_mdb numeric(10,2),
    raw_min_price_from_mdb numeric(10,2),
    past_3_days_min_moving_average numeric(10,2),
    past_5_days_min_moving_average numeric(10,2),
    past_7_days_min_moving_average numeric(10,2),
    past_10_days_min_moving_average numeric(10,2),
    past_15_days_min_moving_average numeric(10,2),
    past_20_days_min_moving_average numeric(10,2),
    past_30_days_min_moving_average numeric(10,2),
    past_45_days_min_moving_average numeric(10,2),
    past_60_days_min_moving_average numeric(10,2),
    past_75_days_min_moving_average numeric(10,2),
    past_90_days_min_moving_average numeric(10,2),
    future_3_days_min_moving_average numeric(10,2),
    future_5_days_min_moving_average numeric(10,2),
    future_7_days_min_moving_average numeric(10,2),
    future_10_days_min_moving_average numeric(10,2),
    future_15_days_min_moving_average numeric(10,2),
    future_20_days_min_moving_average numeric(10,2),
    future_30_days_min_moving_average numeric(10,2),
    future_45_days_min_moving_average numeric(10,2),
    future_60_days_min_moving_average numeric(10,2),
    future_75_days_min_moving_average numeric(10,2),
    future_90_days_min_moving_average numeric(10,2),
    past_3_days_max_moving_average numeric(10,2),
    past_5_days_max_moving_average numeric(10,2),
    past_7_days_max_moving_average numeric(10,2),
    past_10_days_max_moving_average numeric(10,2),
    past_15_days_max_moving_average numeric(10,2),
    past_20_days_max_moving_average numeric(10,2),
    past_30_days_max_moving_average numeric(10,2),
    past_45_days_max_moving_average numeric(10,2),
    past_60_days_max_moving_average numeric(10,2),
    past_75_days_max_moving_average numeric(10,2),
    past_90_days_max_moving_average numeric(10,2),
    future_3_days_max_moving_average numeric(10,2),
    future_5_days_max_moving_average numeric(10,2),
    future_7_days_max_moving_average numeric(10,2),
    future_10_days_max_moving_average numeric(10,2),
    future_15_days_max_moving_average numeric(10,2),
    future_20_days_max_moving_average numeric(10,2),
    future_30_days_max_moving_average numeric(10,2),
    future_45_days_max_moving_average numeric(10,2),
    future_60_days_max_moving_average numeric(10,2),
    future_75_days_max_moving_average numeric(10,2),
    future_90_days_max_moving_average numeric(10,2),
    max_price_from_mdb numeric
);
 9   DROP TABLE analytics_data.model1_jins_market_moving_avg;
       analytics_data         heap r       postgres    false    6            �            1259    16402    commodity_details    TABLE     c  CREATE TABLE exchange_data.commodity_details (
    commodity_name character varying(50),
    type character varying(30),
    exchange character varying(50),
    ticker character varying(20),
    unit character varying(20),
    symbol character varying(20),
    unit_conversion_factor numeric(10,2),
    new_unit character varying(20),
    remarks text
);
 ,   DROP TABLE exchange_data.commodity_details;
       exchange_data         heap r       postgres    false    7            �            1259    16407    commodity_mdb    TABLE     �  CREATE TABLE exchange_data.commodity_mdb (
    date date,
    commodity_name character varying(255),
    type character varying(50),
    exchange character varying(100),
    ticker character varying(20),
    symbol character varying(50),
    unit character varying(50),
    open numeric(10,2),
    close numeric(10,2),
    high numeric(10,2),
    low numeric(10,2),
    average numeric(10,2)
);
 (   DROP TABLE exchange_data.commodity_mdb;
       exchange_data         heap r       postgres    false    7            �            1259    16412    commodity_mdb_date    TABLE     �  CREATE TABLE exchange_data.commodity_mdb_date (
    date date,
    nymeolbt_open_inr_barrel double precision,
    nymeolbt_close_inr_barrel double precision,
    nymeolbt_high_inr_barrel double precision,
    nymeolbt_low_inr_barrel double precision,
    nymeolbt_average_inr_barrel double precision,
    nymeolwt_open_inr_barrel double precision,
    nymeolwt_close_inr_barrel double precision,
    nymeolwt_high_inr_barrel double precision,
    nymeolwt_low_inr_barrel double precision,
    nymeolwt_average_inr_barrel double precision,
    nymengas_open_inr_tonne double precision,
    nymengas_close_inr_tonne double precision,
    nymengas_high_inr_tonne double precision,
    nymengas_low_inr_tonne double precision,
    nymengas_average_inr_tonne double precision,
    comxgold_open_inr_gram double precision,
    comxgold_close_inr_gram double precision,
    comxgold_high_inr_gram double precision,
    comxgold_low_inr_gram double precision,
    comxgold_average_inr_gram double precision,
    comxslvr_open_inr_gram double precision,
    comxslvr_close_inr_gram double precision,
    comxslvr_high_inr_gram double precision,
    comxslvr_low_inr_gram double precision,
    comxslvr_average_inr_gram double precision,
    icexcotn_open_inr_quintal double precision,
    icexcotn_close_inr_quintal double precision,
    icexcotn_high_inr_quintal double precision,
    icexcotn_low_inr_quintal double precision,
    icexcotn_average_inr_quintal double precision,
    klcepmol_open_inr_quintal double precision,
    klcepmol_close_inr_quintal double precision,
    klcepmol_high_inr_quintal double precision,
    klcepmol_low_inr_quintal double precision,
    klcepmol_average_inr_quintal double precision,
    matirpsd_open_inr_quintal double precision,
    matirpsd_close_inr_quintal double precision,
    matirpsd_high_inr_quintal double precision,
    matirpsd_low_inr_quintal double precision,
    matirpsd_average_inr_quintal double precision,
    cbotrice_open_inr_quintal double precision,
    cbotrice_close_inr_quintal double precision,
    cbotrice_high_inr_quintal double precision,
    cbotrice_low_inr_quintal double precision,
    cbotrice_average_inr_quintal double precision,
    cbotcorn_open_inr_quintal double precision,
    cbotcorn_close_inr_quintal double precision,
    cbotcorn_high_inr_quintal double precision,
    cbotcorn_low_inr_quintal double precision,
    cbotcorn_average_inr_quintal double precision,
    cbotweat_open_inr_quintal double precision,
    cbotweat_close_inr_quintal double precision,
    cbotweat_high_inr_quintal double precision,
    cbotweat_low_inr_quintal double precision,
    cbotweat_average_inr_quintal double precision,
    cbotsybn_open_inr_quintal double precision,
    cbotsybn_close_inr_quintal double precision,
    cbotsybn_high_inr_quintal double precision,
    cbotsybn_low_inr_quintal double precision,
    cbotsybn_average_inr_quintal double precision,
    cbotsbol_open_inr_quintal double precision,
    cbotsbol_close_inr_quintal double precision,
    cbotsbol_high_inr_quintal double precision,
    cbotsbol_low_inr_quintal double precision,
    cbotsbol_average_inr_quintal double precision,
    ncdxkpas_open_inr_quintal double precision,
    ncdxkpas_close_inr_quintal double precision,
    ncdxkpas_high_inr_quintal double precision,
    ncdxkpas_low_inr_quintal double precision,
    ncdxkpas_average_inr_quintal double precision,
    ncdxguar_open_inr_quintal double precision,
    ncdxguar_close_inr_quintal double precision,
    ncdxguar_high_inr_quintal double precision,
    ncdxguar_low_inr_quintal double precision,
    ncdxguar_average_inr_quintal double precision,
    ncdxgrgm_open_inr_quintal double precision,
    ncdxgrgm_close_inr_quintal double precision,
    ncdxgrgm_high_inr_quintal double precision,
    ncdxgrgm_low_inr_quintal double precision,
    ncdxgrgm_average_inr_quintal double precision,
    ncdxsnol_open_inr_quintal double precision,
    ncdxsnol_close_inr_quintal double precision,
    ncdxsnol_high_inr_quintal double precision,
    ncdxsnol_low_inr_quintal double precision,
    ncdxsnol_average_inr_quintal double precision,
    ncdxcssd_open_inr_quintal double precision,
    ncdxcssd_close_inr_quintal double precision,
    ncdxcssd_high_inr_quintal double precision,
    ncdxcssd_low_inr_quintal double precision,
    ncdxcssd_average_inr_quintal double precision,
    ncdxctsd_open_inr_quintal double precision,
    ncdxctsd_close_inr_quintal double precision,
    ncdxctsd_high_inr_quintal double precision,
    ncdxctsd_low_inr_quintal double precision,
    ncdxctsd_average_inr_quintal double precision,
    ncdxbrly_open_inr_quintal double precision,
    ncdxbrly_close_inr_quintal double precision,
    ncdxbrly_high_inr_quintal double precision,
    ncdxbrly_low_inr_quintal double precision,
    ncdxbrly_average_inr_quintal double precision,
    ncdxmaze_open_inr_quintal double precision,
    ncdxmaze_close_inr_quintal double precision,
    ncdxmaze_high_inr_quintal double precision,
    ncdxmaze_low_inr_quintal double precision,
    ncdxmaze_average_inr_quintal double precision
);
 -   DROP TABLE exchange_data.commodity_mdb_date;
       exchange_data         heap r       postgres    false    7            �            1259    16415    commodity_raw_master    TABLE     )  CREATE TABLE exchange_data.commodity_raw_master (
    date date NOT NULL,
    commodity character varying(255) NOT NULL,
    unit character varying(255) NOT NULL,
    open numeric(10,2) NOT NULL,
    close numeric(10,2) NOT NULL,
    high numeric(10,2) NOT NULL,
    low numeric(10,2) NOT NULL
);
 /   DROP TABLE exchange_data.commodity_raw_master;
       exchange_data         heap r       postgres    false    7            �            1259    16420    currency_raw_master    TABLE     �   CREATE TABLE exchange_data.currency_raw_master (
    date date NOT NULL,
    open numeric(12,8),
    close numeric(12,8),
    high numeric(12,8),
    low numeric(12,8),
    average numeric(12,8),
    currency character varying(10)
);
 .   DROP TABLE exchange_data.currency_raw_master;
       exchange_data         heap r       postgres    false    7            �            1259    16423 
   eur_master    TABLE     �   CREATE TABLE exchange_data.eur_master (
    date date NOT NULL,
    open numeric(12,8),
    close numeric(12,8),
    high numeric(12,8),
    low numeric(12,8),
    average numeric(12,8),
    currency character varying(10)
);
 %   DROP TABLE exchange_data.eur_master;
       exchange_data         heap r       postgres    false    7            �            1259    16426    exchange_date_log    TABLE     v   CREATE TABLE exchange_data.exchange_date_log (
    date date,
    date_processed integer,
    date_present integer
);
 ,   DROP TABLE exchange_data.exchange_date_log;
       exchange_data         heap r       postgres    false    7            �            1259    16429 
   myr_master    TABLE     �   CREATE TABLE exchange_data.myr_master (
    date date NOT NULL,
    open numeric(12,8),
    close numeric(12,8),
    high numeric(12,8),
    low numeric(12,8),
    average numeric(12,8),
    currency character varying(10)
);
 %   DROP TABLE exchange_data.myr_master;
       exchange_data         heap r       postgres    false    7            �            1259    16432 
   usd_master    TABLE     �   CREATE TABLE exchange_data.usd_master (
    date date NOT NULL,
    open numeric(12,8),
    close numeric(12,8),
    high numeric(12,8),
    low numeric(12,8),
    average numeric(12,8),
    currency character varying(10)
);
 %   DROP TABLE exchange_data.usd_master;
       exchange_data         heap r       postgres    false    7                       1259    32891    interim_mdb    TABLE     y  CREATE TABLE mandi_data.interim_mdb (
    reported_date date,
    state_name character varying(100),
    district_name character varying(100),
    market_name character varying(100),
    variety character varying(100),
    family character varying(100),
    jins_name character varying(100),
    jins_code character varying(50),
    geo_symbol character varying(50),
    jins_symbol character varying(50),
    symbol character varying(50),
    uid character varying(50),
    arrivals_tonnes double precision,
    min_price_rs_quintal double precision,
    max_price_rs_quintal double precision,
    modal_price_rs_quintal double precision,
    min_raw double precision,
    max_raw double precision,
    modal_raw double precision,
    min_price_14_days_moving_avg double precision,
    max_price_14_days_moving_avg double precision,
    modal_price_14_days_moving_avg double precision
);
 #   DROP TABLE mandi_data.interim_mdb;
    
   mandi_data         heap r       postgres    false    8                       1259    32896    interim_raw    TABLE     �  CREATE TABLE mandi_data.interim_raw (
    state_name character varying(255),
    district_name character varying(255),
    market_name character varying(255),
    variety character varying(255),
    family character varying(255),
    arrivals_tonnes double precision,
    min_price_rs_quintal double precision,
    max_price_rs_quintal double precision,
    modal_price_rs_quintal double precision,
    reported_date date,
    jins_name character varying(255),
    jins_code character varying(50)
);
 #   DROP TABLE mandi_data.interim_raw;
    
   mandi_data         heap r       postgres    false    8            �            1259    16435    mandi_date_log    TABLE     p   CREATE TABLE mandi_data.mandi_date_log (
    date date,
    date_processed integer,
    date_present integer
);
 &   DROP TABLE mandi_data.mandi_date_log;
    
   mandi_data         heap r       postgres    false    8            �            1259    16438    mandi_geosymbol    TABLE     \  CREATE TABLE mandi_data.mandi_geosymbol (
    state_name character varying(255),
    district_name character varying(255),
    market_name character varying(255),
    state_code character(2),
    district_code character(2),
    market_code character(2),
    geo_symbol character(6),
    latitude double precision,
    longitude double precision
);
 '   DROP TABLE mandi_data.mandi_geosymbol;
    
   mandi_data         heap r       postgres    false    8            �            1259    16443    mandi_jinssymbol    TABLE     �   CREATE TABLE mandi_data.mandi_jinssymbol (
    jins_name character varying(255) NOT NULL,
    jins_symbol character(3) NOT NULL
);
 (   DROP TABLE mandi_data.mandi_jinssymbol;
    
   mandi_data         heap r       postgres    false    8                        1259    16446 	   mandi_mdb    TABLE     w  CREATE TABLE mandi_data.mandi_mdb (
    reported_date date,
    state_name character varying(100),
    district_name character varying(100),
    market_name character varying(100),
    variety character varying(100),
    family character varying(100),
    jins_name character varying(100),
    jins_code character varying(50),
    geo_symbol character varying(50),
    jins_symbol character varying(50),
    symbol character varying(50),
    uid character varying(50),
    arrivals_tonnes double precision,
    min_price_rs_quintal double precision,
    max_price_rs_quintal double precision,
    modal_price_rs_quintal double precision,
    min_raw double precision,
    max_raw double precision,
    modal_raw double precision,
    min_price_14_days_moving_avg double precision,
    max_price_14_days_moving_avg double precision,
    modal_price_14_days_moving_avg double precision
);
 !   DROP TABLE mandi_data.mandi_mdb;
    
   mandi_data         heap r       postgres    false    8                       1259    16451    mandi_msp_details    TABLE     �   CREATE TABLE mandi_data.mandi_msp_details (
    financial_year character varying(7),
    from_date date,
    to_date date,
    jins character varying(50),
    msp_rs_per_quintal double precision
);
 )   DROP TABLE mandi_data.mandi_msp_details;
    
   mandi_data         heap r       postgres    false    8                       1259    16454    mandi_msp_details_jins    TABLE       CREATE TABLE mandi_data.mandi_msp_details_jins (
    fy character varying(10),
    from_date date,
    to_date date,
    bajra double precision,
    barley double precision,
    jowar_hybrid double precision,
    jowar_maldandi double precision,
    maize double precision,
    paddy_grade_a double precision,
    ragi double precision,
    wheat double precision,
    paddy_common double precision,
    copra_ball double precision,
    copra_milling double precision,
    gram double precision,
    lentil_masur double precision,
    moong double precision,
    tur_arhar double precision,
    urad double precision,
    groundnut double precision,
    nigerseed double precision,
    rapeseed_mustard double precision,
    safflower double precision,
    sesamum double precision,
    soyabean_black double precision,
    soyabean_yellow double precision,
    sunflowerseed double precision,
    jute double precision,
    long_staple_cotton double precision,
    medium_staple_cotton double precision,
    sugarcane double precision
);
 .   DROP TABLE mandi_data.mandi_msp_details_jins;
    
   mandi_data         heap r       postgres    false    8                       1259    16457    mandi_raw_master    TABLE     �  CREATE TABLE mandi_data.mandi_raw_master (
    state_name character varying(255),
    district_name character varying(255),
    market_name character varying(255),
    variety character varying(255),
    family character varying(255),
    arrivals_tonnes double precision,
    min_price_rs_quintal double precision,
    max_price_rs_quintal double precision,
    modal_price_rs_quintal double precision,
    reported_date date,
    jins_name character varying(255),
    jins_code character varying(50)
);
 (   DROP TABLE mandi_data.mandi_raw_master;
    
   mandi_data         heap r       aad1tyanagpal    false    8                       1259    107174    diwali_dates    TABLE     _   CREATE TABLE public.diwali_dates (
    year integer NOT NULL,
    diwali_date date NOT NULL
);
     DROP TABLE public.diwali_dates;
       public         heap r       postgres    false            �           2606    107178    diwali_dates diwali_dates_pkey 
   CONSTRAINT     ^   ALTER TABLE ONLY public.diwali_dates
    ADD CONSTRAINT diwali_dates_pkey PRIMARY KEY (year);
 H   ALTER TABLE ONLY public.diwali_dates DROP CONSTRAINT diwali_dates_pkey;
       public                 postgres    false    264           