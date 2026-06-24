\timing on
\set ON_ERROR_STOP on

SET maintenance_work_mem = '2GB';
SET statement_timeout = 0;

DROP TABLE IF EXISTS public.market_feature_cache_value_keep_20260623;
DROP TABLE IF EXISTS public.market_feature_cache_return_risk_matrix_row_keep_20260623;
DROP TABLE IF EXISTS public.market_feature_cache_return_risk_pairwise_row_keep_20260623;
DROP TABLE IF EXISTS public.market_feature_cache_return_risk_stats_row_keep_20260623;
DROP TABLE IF EXISTS public.market_feature_cache_symbol_keep_20260623;
DROP TABLE IF EXISTS public.market_feature_cache_manifest_keep_20260623;

CREATE TABLE public.market_feature_cache_manifest_keep_20260623
    (LIKE public.market_feature_cache_manifest INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE);
CREATE TABLE public.market_feature_cache_value_keep_20260623
    (LIKE public.market_feature_cache_value INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE);
CREATE TABLE public.market_feature_cache_return_risk_matrix_row_keep_20260623
    (LIKE public.market_feature_cache_return_risk_matrix_row INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE);
CREATE TABLE public.market_feature_cache_return_risk_pairwise_row_keep_20260623
    (LIKE public.market_feature_cache_return_risk_pairwise_row INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE);
CREATE TABLE public.market_feature_cache_return_risk_stats_row_keep_20260623
    (LIKE public.market_feature_cache_return_risk_stats_row INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE);
CREATE TABLE public.market_feature_cache_symbol_keep_20260623
    (LIKE public.market_feature_cache_symbol INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE);

INSERT INTO public.market_feature_cache_manifest_keep_20260623
SELECT *
FROM public.market_feature_cache_manifest
WHERE data_version_id = 'dv-v19-audit-ready-20260615';

ALTER TABLE public.market_feature_cache_manifest_keep_20260623
    ADD CONSTRAINT market_feature_cache_manifest_keep_20260623_pkey PRIMARY KEY (cache_key);

INSERT INTO public.market_feature_cache_value_keep_20260623
SELECT v.*
FROM public.market_feature_cache_value v
JOIN public.market_feature_cache_manifest_keep_20260623 m
  ON m.cache_key = v.cache_key;

INSERT INTO public.market_feature_cache_return_risk_matrix_row_keep_20260623
SELECT r.*
FROM public.market_feature_cache_return_risk_matrix_row r
JOIN public.market_feature_cache_manifest_keep_20260623 m
  ON m.cache_key = r.cache_key;

INSERT INTO public.market_feature_cache_return_risk_pairwise_row_keep_20260623
SELECT r.*
FROM public.market_feature_cache_return_risk_pairwise_row r
JOIN public.market_feature_cache_manifest_keep_20260623 m
  ON m.cache_key = r.cache_key;

INSERT INTO public.market_feature_cache_return_risk_stats_row_keep_20260623
SELECT r.*
FROM public.market_feature_cache_return_risk_stats_row r
JOIN public.market_feature_cache_manifest_keep_20260623 m
  ON m.cache_key = r.cache_key;

INSERT INTO public.market_feature_cache_symbol_keep_20260623
SELECT s.*
FROM public.market_feature_cache_symbol s
JOIN public.market_feature_cache_manifest_keep_20260623 m
  ON m.cache_key = s.cache_key;

ALTER TABLE public.market_feature_cache_value_keep_20260623
    ADD CONSTRAINT market_feature_cache_value_keep_20260623_pkey PRIMARY KEY (cache_key, symbol, trade_date);
CREATE INDEX idx_mfc_value_cache_date_keep_20260623
    ON public.market_feature_cache_value_keep_20260623 USING btree (cache_key, trade_date);
CREATE INDEX idx_mfc_value_symbol_date_keep_20260623
    ON public.market_feature_cache_value_keep_20260623 USING btree (symbol, trade_date);

ALTER TABLE public.market_feature_cache_return_risk_matrix_row_keep_20260623
    ADD CONSTRAINT mfc_return_risk_matrix_row_keep_20260623_pkey PRIMARY KEY (cache_key, score_day, symbol);
CREATE INDEX idx_mfc_return_risk_matrix_symbol_day_keep_20260623
    ON public.market_feature_cache_return_risk_matrix_row_keep_20260623 USING btree (symbol, score_day, cache_key);

ALTER TABLE public.market_feature_cache_return_risk_pairwise_row_keep_20260623
    ADD CONSTRAINT mfc_return_risk_pairwise_row_keep_20260623_pkey PRIMARY KEY (cache_key, score_day, left_symbol, right_symbol);
CREATE INDEX idx_mfc_return_risk_pairwise_left_day_keep_20260623
    ON public.market_feature_cache_return_risk_pairwise_row_keep_20260623 USING btree (left_symbol, score_day, cache_key);
CREATE INDEX idx_mfc_return_risk_pairwise_right_day_keep_20260623
    ON public.market_feature_cache_return_risk_pairwise_row_keep_20260623 USING btree (right_symbol, score_day, cache_key);

ALTER TABLE public.market_feature_cache_return_risk_stats_row_keep_20260623
    ADD CONSTRAINT mfc_return_risk_stats_row_keep_20260623_pkey PRIMARY KEY (cache_key, score_day, symbol);
CREATE INDEX idx_mfc_return_risk_stats_symbol_day_keep_20260623
    ON public.market_feature_cache_return_risk_stats_row_keep_20260623 USING btree (symbol, score_day, cache_key);

ALTER TABLE public.market_feature_cache_symbol_keep_20260623
    ADD CONSTRAINT market_feature_cache_symbol_keep_20260623_pkey PRIMARY KEY (cache_key, symbol);
CREATE INDEX idx_mfc_symbol_symbol_keep_20260623
    ON public.market_feature_cache_symbol_keep_20260623 USING btree (symbol, cache_key);

ALTER TABLE public.market_feature_cache_value_keep_20260623
    ADD CONSTRAINT market_feature_cache_value_cache_key_fkey_keep_20260623
    FOREIGN KEY (cache_key)
    REFERENCES public.market_feature_cache_manifest_keep_20260623(cache_key)
    ON DELETE CASCADE
    NOT VALID;
ALTER TABLE public.market_feature_cache_return_risk_matrix_row_keep_20260623
    ADD CONSTRAINT mfc_return_risk_matrix_row_cache_key_fkey_keep_20260623
    FOREIGN KEY (cache_key)
    REFERENCES public.market_feature_cache_manifest_keep_20260623(cache_key)
    ON DELETE CASCADE
    NOT VALID;
ALTER TABLE public.market_feature_cache_return_risk_pairwise_row_keep_20260623
    ADD CONSTRAINT mfc_return_risk_pairwise_row_cache_key_fkey_keep_20260623
    FOREIGN KEY (cache_key)
    REFERENCES public.market_feature_cache_manifest_keep_20260623(cache_key)
    ON DELETE CASCADE
    NOT VALID;
ALTER TABLE public.market_feature_cache_return_risk_stats_row_keep_20260623
    ADD CONSTRAINT mfc_return_risk_stats_row_cache_key_fkey_keep_20260623
    FOREIGN KEY (cache_key)
    REFERENCES public.market_feature_cache_manifest_keep_20260623(cache_key)
    ON DELETE CASCADE
    NOT VALID;
ALTER TABLE public.market_feature_cache_symbol_keep_20260623
    ADD CONSTRAINT market_feature_cache_symbol_cache_key_fkey_keep_20260623
    FOREIGN KEY (cache_key)
    REFERENCES public.market_feature_cache_manifest_keep_20260623(cache_key)
    ON DELETE CASCADE
    NOT VALID;

CREATE INDEX idx_mfc_manifest_exact_ready_keep_20260623
    ON public.market_feature_cache_manifest_keep_20260623 USING btree
    (feature_kind, data_version_id, start_date, end_date, lookback_days, universe_hash, symbol_count)
    WHERE status = 'ready';

ANALYZE public.market_feature_cache_manifest_keep_20260623;
ANALYZE public.market_feature_cache_value_keep_20260623;
ANALYZE public.market_feature_cache_return_risk_matrix_row_keep_20260623;
ANALYZE public.market_feature_cache_return_risk_pairwise_row_keep_20260623;
ANALYZE public.market_feature_cache_return_risk_stats_row_keep_20260623;
ANALYZE public.market_feature_cache_symbol_keep_20260623;
