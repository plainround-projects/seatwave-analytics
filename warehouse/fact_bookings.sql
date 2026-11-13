-- The warehouse table, the nightly load and the view, for DuckDB 2.0.0 (checked 13 November 2026).
-- Run by NightlyLoadCli (nightly-bookings-load), one statement at a time, through duckdb_jdbc.
-- One row per booking. The archive (lake/bookings/date=YYYY-MM-DD/*.parquet) is written by the
-- consumer group bookings-archiver, at least once, so it may hold a record twice.

CREATE TABLE IF NOT EXISTS fact_bookings (
    booking_id        UUID PRIMARY KEY,
    booking_version   INTEGER NOT NULL,
    event_id          BIGINT NOT NULL,
    customer_id       BIGINT NOT NULL,
    seat_label        VARCHAR NOT NULL,
    confirmed_at      TIMESTAMPTZ NOT NULL,   -- event time of the confirm: finance's minute
    status            VARCHAR NOT NULL CHECK (status IN ('CONFIRMED', 'CANCELED')),
    status_changed_at TIMESTAMPTZ NOT NULL,
    loaded_on         DATE NOT NULL
);

-- nightly-bookings-load for one day. Before this statement the job binds the day from its schedule
-- as a parameter (SET VARIABLE load_day = ?::DATE), never from text a user typed, and points
-- file_search_path at the folder that holds lake/, so the relative path below resolves.

INSERT INTO fact_bookings
SELECT booking_id, booking_version, event_id, customer_id, seat_label,
       -- A cancel whose confirm was loaded on an earlier day always conflicts below, and the
       -- update leaves confirmed_at alone, so this stand-in value is never stored.
       coalesce(first_confirm_at, occurred_at) AS confirmed_at,
       CASE type WHEN 'BookingConfirmed' THEN 'CONFIRMED' ELSE 'CANCELED' END AS status,
       occurred_at AS status_changed_at,
       getvariable('load_day') AS loaded_on
FROM (
    SELECT *,
           min(occurred_at) FILTER (WHERE type = 'BookingConfirmed')
               OVER (PARTITION BY booking_id) AS first_confirm_at
    FROM read_parquet('lake/bookings/*/*.parquet', hive_partitioning = true)
    WHERE date = getvariable('load_day')
    -- One row per booking: DuckDB applies only one update per conflicting key in a statement,
    -- and not necessarily the last, so duplicates and older versions are removed first.
    QUALIFY row_number() OVER (PARTITION BY booking_id ORDER BY booking_version DESC) = 1
) AS latest
WHERE first_confirm_at IS NOT NULL
   OR booking_id IN (SELECT booking_id FROM fact_bookings)
ON CONFLICT (booking_id) DO UPDATE SET
    booking_version   = EXCLUDED.booking_version,
    status            = EXCLUDED.status,
    status_changed_at = EXCLUDED.status_changed_at
WHERE EXCLUDED.booking_version > fact_bookings.booking_version;   -- equal or older: no change

-- What finance and the reconciliation read.
CREATE OR REPLACE VIEW sales_per_minute AS
SELECT event_id,
       date_trunc('minute', confirmed_at) AS minute,
       count(*) AS confirms,
       count(*) FILTER (WHERE status = 'CONFIRMED') AS still_confirmed
FROM fact_bookings
GROUP BY ALL;
