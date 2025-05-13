INSERT INTO exchange_data.commodity_mdb_date (
  date, NYMEOLBT_Open_INR_barrel, NYMEOLBT_Close_INR_barrel, NYMEOLBT_High_INR_barrel, NYMEOLBT_Low_INR_barrel, NYMEOLBT_Average_INR_barrel,
  NYMEOLWT_Open_INR_barrel, NYMEOLWT_Close_INR_barrel, NYMEOLWT_High_INR_barrel, NYMEOLWT_Low_INR_barrel, NYMEOLWT_Average_INR_barrel,
  NYMENGAS_Open_INR_tonne, NYMENGAS_Close_INR_tonne, NYMENGAS_High_INR_tonne, NYMENGAS_Low_INR_tonne, NYMENGAS_Average_INR_tonne,
  COMXGOLD_Open_INR_gram, COMXGOLD_Close_INR_gram, COMXGOLD_High_INR_gram, COMXGOLD_Low_INR_gram, COMXGOLD_Average_INR_gram,
  COMXSLVR_Open_INR_gram, COMXSLVR_Close_INR_gram, COMXSLVR_High_INR_gram, COMXSLVR_Low_INR_gram, COMXSLVR_Average_INR_gram,
  ICEXCOTN_Open_INR_quintal, ICEXCOTN_Close_INR_quintal, ICEXCOTN_High_INR_quintal, ICEXCOTN_Low_INR_quintal, ICEXCOTN_Average_INR_quintal,
  KLCEPMOL_Open_INR_quintal, KLCEPMOL_Close_INR_quintal, KLCEPMOL_High_INR_quintal, KLCEPMOL_Low_INR_quintal, KLCEPMOL_Average_INR_quintal,
  MATIRPSD_Open_INR_quintal, MATIRPSD_Close_INR_quintal, MATIRPSD_High_INR_quintal, MATIRPSD_Low_INR_quintal, MATIRPSD_Average_INR_quintal,
  CBOTRICE_Open_INR_quintal, CBOTRICE_Close_INR_quintal, CBOTRICE_High_INR_quintal, CBOTRICE_Low_INR_quintal, CBOTRICE_Average_INR_quintal,
  CBOTCORN_Open_INR_quintal, CBOTCORN_Close_INR_quintal, CBOTCORN_High_INR_quintal, CBOTCORN_Low_INR_quintal, CBOTCORN_Average_INR_quintal,
  CBOTWEAT_Open_INR_quintal, CBOTWEAT_Close_INR_quintal, CBOTWEAT_High_INR_quintal, CBOTWEAT_Low_INR_quintal, CBOTWEAT_Average_INR_quintal,
  CBOTSYBN_Open_INR_quintal, CBOTSYBN_Close_INR_quintal, CBOTSYBN_High_INR_quintal, CBOTSYBN_Low_INR_quintal, CBOTSYBN_Average_INR_quintal,
  CBOTSBOL_Open_INR_quintal, CBOTSBOL_Close_INR_quintal, CBOTSBOL_High_INR_quintal, CBOTSBOL_Low_INR_quintal, CBOTSBOL_Average_INR_quintal,
  NCDXKPAS_Open_INR_quintal, NCDXKPAS_Close_INR_quintal, NCDXKPAS_High_INR_quintal, NCDXKPAS_Low_INR_quintal, NCDXKPAS_Average_INR_quintal,
  NCDXGUAR_Open_INR_quintal, NCDXGUAR_Close_INR_quintal, NCDXGUAR_High_INR_quintal, NCDXGUAR_Low_INR_quintal, NCDXGUAR_Average_INR_quintal,
  NCDXGRGM_Open_INR_quintal, NCDXGRGM_Close_INR_quintal, NCDXGRGM_High_INR_quintal, NCDXGRGM_Low_INR_quintal, NCDXGRGM_Average_INR_quintal,
  NCDXSNOL_Open_INR_quintal, NCDXSNOL_Close_INR_quintal, NCDXSNOL_High_INR_quintal, NCDXSNOL_Low_INR_quintal, NCDXSNOL_Average_INR_quintal,
  NCDXCSSD_Open_INR_quintal, NCDXCSSD_Close_INR_quintal, NCDXCSSD_High_INR_quintal, NCDXCSSD_Low_INR_quintal, NCDXCSSD_Average_INR_quintal,
  NCDXCTSD_Open_INR_quintal, NCDXCTSD_Close_INR_quintal, NCDXCTSD_High_INR_quintal, NCDXCTSD_Low_INR_quintal, NCDXCTSD_Average_INR_quintal,
  NCDXBRLY_Open_INR_quintal, NCDXBRLY_Close_INR_quintal, NCDXBRLY_High_INR_quintal, NCDXBRLY_Low_INR_quintal, NCDXBRLY_Average_INR_quintal,
  NCDXMAZE_Open_INR_quintal, NCDXMAZE_Close_INR_quintal, NCDXMAZE_High_INR_quintal, NCDXMAZE_Low_INR_quintal, NCDXMAZE_Average_INR_quintal
)
SELECT
  c1.date,
  c1.open AS NYMEOLBT_Open_INR_barrel, c1.close AS NYMEOLBT_Close_INR_barrel, c1.high AS NYMEOLBT_High_INR_barrel, c1.low AS NYMEOLBT_Low_INR_barrel, c1.average AS NYMEOLBT_Average_INR_barrel,
  c2.open AS NYMEOLWT_Open_INR_barrel, c2.close AS NYMEOLWT_Close_INR_barrel, c2.high AS NYMEOLWT_High_INR_barrel, c2.low AS NYMEOLWT_Low_INR_barrel, c2.average AS NYMEOLWT_Average_INR_barrel,
  c3.open AS NYMENGAS_Open_INR_tonne, c3.close AS NYMENGAS_Close_INR_tonne, c3.high AS NYMENGAS_High_INR_tonne, c3.low AS NYMENGAS_Low_INR_tonne, c3.average AS NYMENGAS_Average_INR_tonne,
  c4.open AS COMXGOLD_Open_INR_gram, c4.close AS COMXGOLD_Close_INR_gram, c4.high AS COMXGOLD_High_INR_gram, c4.low AS COMXGOLD_Low_INR_gram, c4.average AS COMXGOLD_Average_INR_gram,
  c5.open AS COMXSLVR_Open_INR_gram, c5.close AS COMXSLVR_Close_INR_gram, c5.high AS COMXSLVR_High_INR_gram, c5.low AS COMXSLVR_Low_INR_gram, c5.average AS COMXSLVR_Average_INR_gram,
  c6.open AS ICEXCOTN_Open_INR_quintal, c6.close AS ICEXCOTN_Close_INR_quintal, c6.high AS ICEXCOTN_High_INR_quintal, c6.low AS ICEXCOTN_Low_INR_quintal, c6.average AS ICEXCOTN_Average_INR_quintal,
  c7.open AS KLCEPMOL_Open_INR_quintal, c7.close AS KLCEPMOL_Close_INR_quintal, c7.high AS KLCEPMOL_High_INR_quintal, c7.low AS KLCEPMOL_Low_INR_quintal, c7.average AS KLCEPMOL_Average_INR_quintal,
  c8.open AS MATIRPSD_Open_INR_quintal, c8.close AS MATIRPSD_Close_INR_quintal, c8.high AS MATIRPSD_High_INR_quintal, c8.low AS MATIRPSD_Low_INR_quintal, c8.average AS MATIRPSD_Average_INR_quintal,
  c9.open AS CBOTRICE_Open_INR_quintal, c9.close AS CBOTRICE_Close_INR_quintal, c9.high AS CBOTRICE_High_INR_quintal, c9.low AS CBOTRICE_Low_INR_quintal, c9.average AS CBOTRICE_Average_INR_quintal,
  c10.open AS CBOTCORN_Open_INR_quintal, c10.close AS CBOTCORN_Close_INR_quintal, c10.high AS CBOTCORN_High_INR_quintal, c10.low AS CBOTCORN_Low_INR_quintal, c10.average AS CBOTCORN_Average_INR_quintal,
  c11.open AS CBOTWEAT_Open_INR_quintal, c11.close AS CBOTWEAT_Close_INR_quintal, c11.high AS CBOTWEAT_High_INR_quintal, c11.low AS CBOTWEAT_Low_INR_quintal, c11.average AS CBOTWEAT_Average_INR_quintal,
  c12.open AS CBOTSYBN_Open_INR_quintal, c12.close AS CBOTSYBN_Close_INR_quintal, c12.high AS CBOTSYBN_High_INR_quintal, c12.low AS CBOTSYBN_Low_INR_quintal, c12.average AS CBOTSYBN_Average_INR_quintal,
  c13.open AS CBOTSBOL_Open_INR_quintal, c13.close AS CBOTSBOL_Close_INR_quintal, c13.high AS CBOTSBOL_High_INR_quintal, c13.low AS CBOTSBOL_Low_INR_quintal, c13.average AS CBOTSBOL_Average_INR_quintal,
  c14.open AS NCDXKPAS_Open_INR_quintal, c14.close AS NCDXKPAS_Close_INR_quintal, c14.high AS NCDXKPAS_High_INR_quintal, c14.low AS NCDXKPAS_Low_INR_quintal, c14.average AS NCDXKPAS_Average_INR_quintal,
  c15.open AS NCDXGUAR_Open_INR_quintal, c15.close AS NCDXGUAR_Close_INR_quintal, c15.high AS NCDXGUAR_High_INR_quintal, c15.low AS NCDXGUAR_Low_INR_quintal, c15.average AS NCDXGUAR_Average_INR_quintal,
  c16.open AS NCDXGRGM_Open_INR_quintal, c16.close AS NCDXGRGM_Close_INR_quintal, c16.high AS NCDXGRGM_High_INR_quintal, c16.low AS NCDXGRGM_Low_INR_quintal, c16.average AS NCDXGRGM_Average_INR_quintal,
  c17.open AS NCDXSNOL_Open_INR_quintal, c17.close AS NCDXSNOL_Close_INR_quintal, c17.high AS NCDXSNOL_High_INR_quintal, c17.low AS NCDXSNOL_Low_INR_quintal, c17.average AS NCDXSNOL_Average_INR_quintal,
  c18.open AS NCDXCSSD_Open_INR_quintal, c18.close AS NCDXCSSD_Close_INR_quintal, c18.high AS NCDXCSSD_High_INR_quintal, c18.low AS NCDXCSSD_Low_INR_quintal, c18.average AS NCDXCSSD_Average_INR_quintal,
  c19.open AS NCDXCTSD_Open_INR_quintal, c19.close AS NCDXCTSD_Close_INR_quintal, c19.high AS NCDXCTSD_High_INR_quintal, c19.low AS NCDXCTSD_Low_INR_quintal, c19.average AS NCDXCTSD_Average_INR_quintal,
  c20.open AS NCDXBRLY_Open_INR_quintal, c20.close AS NCDXBRLY_Close_INR_quintal, c20.high AS NCDXBRLY_High_INR_quintal, c20.low AS NCDXBRLY_Low_INR_quintal, c20.average AS NCDXBRLY_Average_INR_quintal,
  c21.open AS NCDXMAZE_Open_INR_quintal, c21.close AS NCDXMAZE_Close_INR_quintal, c21.high AS NCDXMAZE_High_INR_quintal, c21.low AS NCDXMAZE_Low_INR_quintal, c21.average AS NCDXMAZE_Average_INR_quintal
FROM exchange_data.commodity_mdb c1
JOIN exchange_data.commodity_mdb c2 ON c1.date = c2.date AND c2.symbol = 'NYMEOLWT'
JOIN exchange_data.commodity_mdb c3 ON c1.date = c3.date AND c3.symbol = 'NYMENGAS'
JOIN exchange_data.commodity_mdb c4 ON c1.date = c4.date AND c4.symbol = 'COMXGOLD'
JOIN exchange_data.commodity_mdb c5 ON c1.date = c5.date AND c5.symbol = 'COMXSLVR'
JOIN exchange_data.commodity_mdb c6 ON c1.date = c6.date AND c6.symbol = 'ICEXCOTN'
JOIN exchange_data.commodity_mdb c7 ON c1.date = c7.date AND c7.symbol = 'KLCEPMOL'
JOIN exchange_data.commodity_mdb c8 ON c1.date = c8.date AND c8.symbol = 'MATIRPSD'
JOIN exchange_data.commodity_mdb c9 ON c1.date = c9.date AND c9.symbol = 'CBOTRICE'
JOIN exchange_data.commodity_mdb c10 ON c1.date = c10.date AND c10.symbol = 'CBOTCORN'
JOIN exchange_data.commodity_mdb c11 ON c1.date = c11.date AND c11.symbol = 'CBOTWEAT'
JOIN exchange_data.commodity_mdb c12 ON c1.date = c12.date AND c12.symbol = 'CBOTSYBN'
JOIN exchange_data.commodity_mdb c13 ON c1.date = c13.date AND c13.symbol = 'CBOTSBOL'
JOIN exchange_data.commodity_mdb c14 ON c1.date = c14.date AND c14.symbol = 'NCDXKPAS'
JOIN exchange_data.commodity_mdb c15 ON c1.date = c15.date AND c15.symbol = 'NCDXGUAR'
JOIN exchange_data.commodity_mdb c16 ON c1.date = c16.date AND c16.symbol = 'NCDXGRGM'
JOIN exchange_data.commodity_mdb c17 ON c1.date = c17.date AND c17.symbol = 'NCDXSNOL'
JOIN exchange_data.commodity_mdb c18 ON c1.date = c18.date AND c18.symbol = 'NCDXCSSD'
JOIN exchange_data.commodity_mdb c19 ON c1.date = c19.date AND c19.symbol = 'NCDXCTSD'
JOIN exchange_data.commodity_mdb c20 ON c1.date = c20.date AND c20.symbol = 'NCDXBRLY'
JOIN exchange_data.commodity_mdb c21 ON c1.date = c21.date AND c21.symbol = 'NCDXMAZE';
