-- Strategy candidate tier governance.
-- Keeps production/research strategy configs from being mislabelled as professional candidates.

ALTER TABLE strategy_config
    ADD COLUMN IF NOT EXISTS candidate_tier varchar(32) NOT NULL DEFAULT 'research_baseline';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'strategy_config_candidate_tier_check'
    ) THEN
        ALTER TABLE strategy_config
            ADD CONSTRAINT strategy_config_candidate_tier_check
            CHECK (candidate_tier IN (
                'research_baseline',
                'defensive_candidate',
                'professional_observation',
                'professional_elite'
            ));
    END IF;
END $$;

COMMENT ON COLUMN strategy_config.candidate_tier IS
    'Strategy promotion tier: research_baseline / defensive_candidate / professional_observation / professional_elite';

UPDATE strategy_config
   SET candidate_tier = 'defensive_candidate'
 WHERE strategy_id IN ('v19', 'v20')
   AND status = 'active'
   AND candidate_tier = 'research_baseline';
