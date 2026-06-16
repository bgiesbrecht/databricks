-- setup_bg_solar_panels.sql
-- Provisions the `bg.solar.panels` demo table that pairs with
-- solar_panels_config.json. Run this once against the SQL warehouse that the
-- Genie Space will use (Databricks SQL editor or `databricks api`).
--
-- To target a different catalog/schema/table, generate a customized copy:
--
--     sed -e 's/bg\.solar/my_cat.my_sch/g' \
--         -e 's/\.panels/\.my_table/g' \
--         setup_bg_solar_panels.sql > my_setup.sql
--
-- and pass the matching values to demo.py:
--
--     python3 demo.py --warehouse-id ... \
--         --catalog my_cat --schema my_sch --table my_table

CREATE SCHEMA IF NOT EXISTS bg.solar
  COMMENT 'Demo schema for the Solar Panel Installations Genie Space.';

CREATE OR REPLACE TABLE bg.solar.panels (
    panel_id     BIGINT NOT NULL  COMMENT 'Unique identifier for an installed panel.',
    serial_no    STRING           COMMENT 'Manufacturer serial number.',
    wattage      BIGINT NOT NULL  COMMENT 'Panel wattage rating in watts (W).',
    installed_at DATE   NOT NULL  COMMENT 'Date the panel was installed in the field.'
)
COMMENT 'Installed solar panels. One row per panel. Every row represents a panel that is currently installed.';

-- 200 demo panels installed from 2023-01-01 to ~April 2026 at ~6-day intervals.
-- Most panels are residential 300–500W; every 20th panel is high-wattage
-- (>= 5500W) to give the "High Wattage" filter snippet something to engage.
INSERT INTO bg.solar.panels
SELECT
    id                                                                   AS panel_id,
    CONCAT('SP-', LPAD(CAST(id AS STRING), 5, '0'))                      AS serial_no,
    CASE
        WHEN id % 20 = 0 THEN 5500 + CAST(MOD(id * 7, 2500) AS BIGINT)
        WHEN id %  5 = 0 THEN 400  + CAST(MOD(id * 13, 200)  AS BIGINT)
        ELSE                  300  + CAST(MOD(id * 17, 200)  AS BIGINT)
    END                                                                  AS wattage,
    DATE_ADD(DATE '2023-01-01', CAST(id * 6 AS INT))                     AS installed_at
FROM (SELECT EXPLODE(SEQUENCE(1, 200)) AS id);

-- Sanity check (optional — comment out if running non-interactively):
SELECT
    COUNT(*)                                          AS panel_count,
    MIN(installed_at)                                 AS earliest_install,
    MAX(installed_at)                                 AS latest_install,
    ROUND(SUM(wattage) / 1000.0, 1)                   AS installed_capacity_kw,
    SUM(CASE WHEN wattage >= 5000 THEN 1 ELSE 0 END)  AS high_wattage_panels
FROM bg.solar.panels;
