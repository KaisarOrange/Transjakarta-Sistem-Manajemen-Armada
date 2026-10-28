package tracking

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/KaisarOrange/Transjakarta-Sistem-Manajemen-Armada/internal/geofence"
)

type Repository struct{ pool *pgxpool.Pool }

func NewRepository(pool *pgxpool.Pool) *Repository { return &Repository{pool: pool} }

func (r *Repository) Ingest(ctx context.Context, l Location, decide Decide) (bool, error) {
	var inserted bool
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`INSERT INTO vehicle_locations (vehicle_id, latitude, longitude, timestamp)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT (vehicle_id, timestamp) DO NOTHING`,
			l.VehicleID, l.Latitude, l.Longitude, l.Timestamp)
		if err != nil {
			return fmt.Errorf("insert location: %w", err)
		}
		if inserted = tag.RowsAffected() == 1; !inserted {
			return nil
		}

		if _, err := tx.Exec(ctx,
			`INSERT INTO vehicle_geofence_state (vehicle_id, inside, last_timestamp)
			 VALUES ($1, false, 0) ON CONFLICT (vehicle_id) DO NOTHING`, l.VehicleID); err != nil {
			return fmt.Errorf("init geofence state: %w", err)
		}
		var prev geofence.State
		if err := tx.QueryRow(ctx,
			`SELECT inside, last_timestamp FROM vehicle_geofence_state
			 WHERE vehicle_id = $1 FOR UPDATE`, l.VehicleID).Scan(&prev.Inside, &prev.LastTimestamp); err != nil {
			return fmt.Errorf("lock geofence state: %w", err)
		}

		next, event := decide(prev)
		if next != prev {
			if _, err := tx.Exec(ctx,
				`UPDATE vehicle_geofence_state SET inside = $2, last_timestamp = $3, updated_at = now()
				 WHERE vehicle_id = $1`, l.VehicleID, next.Inside, next.LastTimestamp); err != nil {
				return fmt.Errorf("update geofence state: %w", err)
			}
		}
		if event != nil {
			body, err := json.Marshal(event)
			if err != nil {
				return fmt.Errorf("encode event: %w", err)
			}
			if _, err := tx.Exec(ctx,
				`INSERT INTO geofence_outbox (message_id, payload) VALUES ($1, $2)
				 ON CONFLICT (message_id) DO NOTHING`, event.MessageID(), string(body)); err != nil {
				return fmt.Errorf("queue event: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return false, permanentAsInvalid(err)
	}
	return inserted, nil
}

func (r *Repository) Latest(ctx context.Context, vehicleID string) (Location, error) {
	var l Location
	err := r.pool.QueryRow(ctx,
		`SELECT vehicle_id, latitude, longitude, timestamp
		 FROM vehicle_locations WHERE vehicle_id = $1
		 ORDER BY timestamp DESC LIMIT 1`, vehicleID).
		Scan(&l.VehicleID, &l.Latitude, &l.Longitude, &l.Timestamp)
	if errors.Is(err, pgx.ErrNoRows) {
		return Location{}, ErrNotFound
	}
	if err != nil {
		return Location{}, fmt.Errorf("latest location: %w", err)
	}
	return l, nil
}

func (r *Repository) History(ctx context.Context, vehicleID string, start, end int64, limit int) ([]Location, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT vehicle_id, latitude, longitude, timestamp
		 FROM vehicle_locations
		 WHERE vehicle_id = $1 AND timestamp BETWEEN $2 AND $3
		 ORDER BY timestamp ASC LIMIT $4`, vehicleID, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("location history: %w", err)
	}
	out, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Location, error) {
		var l Location
		err := row.Scan(&l.VehicleID, &l.Latitude, &l.Longitude, &l.Timestamp)
		return l, err
	})
	if err != nil {
		return nil, fmt.Errorf("location history: %w", err)
	}
	if out == nil {
		out = []Location{}
	}
	return out, nil
}

func permanentAsInvalid(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (strings.HasPrefix(pgErr.Code, "22") || strings.HasPrefix(pgErr.Code, "23")) {
		return invalid("reading rejected by the database (SQLSTATE %s)", pgErr.Code)
	}
	return err
}
