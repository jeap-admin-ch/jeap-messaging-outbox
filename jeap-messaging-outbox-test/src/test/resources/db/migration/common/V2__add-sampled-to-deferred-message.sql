-- Adds the sampling decision captured from the origin trace so replay through TraceContextUpdater preserves it.
-- Rows migrated from a pre-OTel jEAP version carry NULL here and are treated as sampled (legacy default) on replay.
ALTER TABLE deferred_message ADD COLUMN sampled boolean;
