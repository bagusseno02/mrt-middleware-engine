package model

import "time"

type Values struct {
	CurrentA      *float64 `json:"current_a_a"`
	CurrentB      *float64 `json:"current_b_a"`
	CurrentC      *float64 `json:"current_c_a"`
	VoltageAB     *float64 `json:"voltage_ab_v"`
	VoltageBC     *float64 `json:"voltage_bc_v"`
	VoltageCA     *float64 `json:"voltage_ca_v"`
	VoltageAN     *float64 `json:"voltage_an_v"`
	VoltageBN     *float64 `json:"voltage_bn_v"`
	VoltageCN     *float64 `json:"voltage_cn_v"`
	ActivePower   *float64 `json:"active_power_kw"`
	ReactivePower *float64 `json:"reactive_power_kvar"`
	ApparentPower *float64 `json:"apparent_power_kva"`
	PowerFactor   *float64 `json:"power_factor"`
	Frequency     *float64 `json:"frequency_hz"`
	// Decimal strings preserve the full INT64 Wh counter through JSON and PostgreSQL.
	EnergyImport *string `json:"energy_import_wh"`
	EnergyExport *string `json:"energy_export_wh"`
}
type Measurement struct {
	ID          string            `json:"id"`
	MeterID     string            `json:"meter_id"`
	ObservedAt  time.Time         `json:"observed_at"`
	CompletedAt time.Time         `json:"completed_at"`
	Quality     string            `json:"quality"`
	Errors      map[string]string `json:"errors"`
	Values      Values            `json:"values"`
}
type Cursor struct {
	Time time.Time `json:"time"`
	ID   string    `json:"id"`
}
type Query struct {
	From, To time.Time
	Limit    int
	Cursor   *Cursor
}
