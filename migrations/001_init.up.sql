CREATE TABLE meters (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    location TEXT NOT NULL DEFAULT '',
    poll_interval_ms BIGINT NOT NULL CHECK (poll_interval_ms > 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE measurements (
    id UUID PRIMARY KEY,
    meter_id TEXT NOT NULL REFERENCES meters(id),
    observed_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL CHECK (completed_at >= observed_at),
    quality TEXT NOT NULL CHECK (quality IN ('good', 'partial', 'failed')),
    errors JSONB NOT NULL DEFAULT '{}'::jsonb,
    current_a_a DOUBLE PRECISION,
    current_b_a DOUBLE PRECISION,
    current_c_a DOUBLE PRECISION,
    voltage_ab_v DOUBLE PRECISION,
    voltage_bc_v DOUBLE PRECISION,
    voltage_ca_v DOUBLE PRECISION,
    voltage_an_v DOUBLE PRECISION,
    voltage_bn_v DOUBLE PRECISION,
    voltage_cn_v DOUBLE PRECISION,
    active_power_kw DOUBLE PRECISION,
    reactive_power_kvar DOUBLE PRECISION,
    apparent_power_kva DOUBLE PRECISION,
    power_factor DOUBLE PRECISION CHECK (power_factor BETWEEN -1 AND 1),
    frequency_hz DOUBLE PRECISION,
    energy_import_wh BIGINT CHECK (energy_import_wh >= 0),
    energy_export_wh BIGINT CHECK (energy_export_wh >= 0)
);
CREATE INDEX measurements_meter_time_idx
    ON measurements (meter_id, observed_at DESC, id DESC);
