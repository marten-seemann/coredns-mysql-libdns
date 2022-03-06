package coredns_mysql_libdns

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/libdns/libdns"
)

// Provider facilitates DNS record manipulation with CoreDNS when using the MySQL plugin.
type Provider struct {
	DSN string

	mx sync.Mutex
	db *sql.DB // lazily initialized and accessed via getDB()
}

func (p *Provider) getDB() (*sql.DB, error) {
	p.mx.Lock()
	db := p.db
	p.mx.Unlock()
	if db != nil {
		return db, nil
	}

	db, err := sql.Open("mysql", p.DSN)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(time.Minute)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	p.mx.Lock()
	p.db = db
	p.mx.Unlock()
	return db, nil
}

// GetRecords lists all the records in the zone.
func (p *Provider) GetRecords(ctx context.Context, zone string) ([]libdns.Record, error) {
	db, err := p.getDB()
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, "SELECT (id, name, ttl, content, record_type) FROM coredns_records WHERE zone=?", zone)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []libdns.Record
	for rows.Next() {
		var (
			record     libdns.Record
			ttlSeconds int64
			content    string
		)
		if err := rows.Scan(&record.ID, &record.Name, &ttlSeconds, &content, &record.Type); err != nil {
			return records, err
		}
		record.TTL = time.Duration(ttlSeconds) * time.Second

		var value string
		switch record.Type {
		case "A":
			value, err = unmarshalARecord([]byte(content))
		case "TXT":
			value, err = unmarshalTXTRecord([]byte(content))
		default:
			return records, fmt.Errorf("unsupported record type: %s", record.Type)
		}
		if err != nil {
			return records, err
		}
		record.Value = value
		records = append(records, record)
	}
	return records, nil
}

// AppendRecords adds records to the zone. It returns the records that were added.
func (p *Provider) AppendRecords(ctx context.Context, zone string, records []libdns.Record) ([]libdns.Record, error) {
	db, err := p.getDB()
	if err != nil {
		return nil, err
	}
	stmt, err := db.PrepareContext(ctx, "INSERT INTO coredns_records (zone, name, ttl, content, record_type) VALUES(?, ?, ?, ?, ?)")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	insertedRecords := make([]libdns.Record, 0, len(records))
	for _, record := range records {
		value, err := marshalRecord(record)
		if err != nil {
			return insertedRecords, err
		}
		ttl := int64(record.TTL.Seconds())
		result, err := stmt.ExecContext(ctx, zone, record.Name, ttl, value, record.Type)
		if err != nil {
			return insertedRecords, err
		}
		id, err := result.LastInsertId()
		if err != nil {
			return insertedRecords, err
		}
		insertedRecords = append(insertedRecords, libdns.Record{
			ID:       fmt.Sprintf("%d", id),
			Type:     record.Type,
			Name:     record.Name,
			Value:    record.Value,
			TTL:      time.Duration(ttl) * time.Second,
			Priority: record.Priority,
		})
	}
	return insertedRecords, nil
}

// SetRecords sets the records in the zone, either by updating existing records or creating new ones.
// It returns the updated records.
func (p *Provider) SetRecords(ctx context.Context, zone string, records []libdns.Record) ([]libdns.Record, error) {
	return nil, fmt.Errorf("TODO: not implemented")
}

// DeleteRecords deletes the records from the zone. It returns the records that were deleted.
func (p *Provider) DeleteRecords(ctx context.Context, zone string, records []libdns.Record) ([]libdns.Record, error) {
	db, err := p.getDB()
	if err != nil {
		return nil, err
	}
	stmt, err := db.PrepareContext(ctx, "DELETE FROM coredns_records WHERE id=?")
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	for i, record := range records {
		if record.ID != "" {
			id, err := strconv.ParseInt(record.ID, 10, 64)
			if err != nil {
				return records[:i], err
			}
			if _, err := stmt.ExecContext(ctx, id); err != nil {
				return records[:i], err
			}
		}
		// TODO: handle records without ID
	}
	return records, nil
}

// Interface guards
var (
	_ libdns.RecordGetter   = (*Provider)(nil)
	_ libdns.RecordAppender = (*Provider)(nil)
	_ libdns.RecordSetter   = (*Provider)(nil)
	_ libdns.RecordDeleter  = (*Provider)(nil)
)
