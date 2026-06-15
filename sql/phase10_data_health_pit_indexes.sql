-- Data health page PIT-audit performance indexes.
--
-- These indexes keep strict "available_at must not be after trade_date" checks
-- fast without weakening PIT validation. Normal PIT-compliant data produces
-- very small or empty partial indexes, so SELECT EXISTS probes can avoid
-- scanning full factor/prediction histories.
--
-- Timescale hypertables do not support CREATE INDEX CONCURRENTLY. Run in a
-- maintenance window on populated databases because model_prediction may have
-- many chunks and index creation can block writes while building.

SET statement_timeout = 0;

CREATE INDEX IF NOT EXISTS idx_multi_factor_value_future_available_at
    ON public.multi_factor_value (combo_name, version, trade_date)
    WHERE available_at > trade_date;

CREATE INDEX IF NOT EXISTS idx_model_prediction_future_available_at
    ON public.model_prediction (prediction_set_id, trade_date)
    WHERE available_at > trade_date;
