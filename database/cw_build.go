package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/tdx"
)

type CwRebuildBatch struct {
	Records []tdx.CWRecord
}

func RebuildCwTable(ctx context.Context, db *sql.DB, batches <-chan CwRebuildBatch) error {
	stageName := CaiwuSchema.Name + "_stage"
	stage := TableSchema{
		Name:    stageName,
		Columns: append([]string(nil), CaiwuSchema.Columns...),
		Keys:    append([]string(nil), CaiwuSchema.Keys...),
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("cw rebuild: get conn: %w", err)
	}
	defer conn.Close()

	if err := execTableDDL(ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", stageName)); err != nil {
		return fmt.Errorf("cw rebuild: drop stage: %w", err)
	}
	if err := createTableOnConn(ctx, conn, stage); err != nil {
		return fmt.Errorf("cw rebuild: create stage: %w", err)
	}

	if err := conn.Raw(func(dc any) error {
		driverConn, ok := dc.(driver.Conn)
		if !ok {
			return fmt.Errorf("cw rebuild: unexpected driver conn type %T", dc)
		}

		appender, err := duckdb.NewAppenderFromConn(driverConn, "", stageName)
		if err != nil {
			return fmt.Errorf("cw rebuild: new appender: %w", err)
		}
		closed := false
		defer func() {
			if closed {
				return
			}
			_ = appender.Close()
		}()

		columnCount := len(stage.Columns)
		fieldOffset := 3
		fieldCount := columnCount - fieldOffset
		if fieldCount != len(cwbase) {
			return fmt.Errorf("cw rebuild: schema mismatch: table fields=%d meta fields=%d", fieldCount, len(cwbase))
		}

		rowValues := make([]driver.Value, columnCount)

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case batch, ok := <-batches:
				if !ok {
					if err := appender.Close(); err != nil {
						return fmt.Errorf("cw rebuild: close appender: %w", err)
					}
					closed = true
					return nil
				}

				for _, record := range batch.Records {
					if record.Code == "" {
						continue
					}

					reportDate, err := parseReportDate(record.ReportDate)
					if err != nil {
						return fmt.Errorf("cw rebuild: invalid report date %d: %w", record.ReportDate, err)
					}

					rowValues[0] = record.Code
					rowValues[1] = reportDate

					if t, err := parseAnnounceDate(record.AnnounceDate); err == nil {
						rowValues[2] = t
					} else {
						rowValues[2] = nil
					}

					for i, column := range cwbase {
						if int(column.idx) < len(record.Values) {
							rowValues[fieldOffset+i] = float64(record.Values[column.idx])
						} else {
							rowValues[fieldOffset+i] = nil
						}
					}

					if err := appender.AppendRow(rowValues...); err != nil {
						return fmt.Errorf("cw rebuild: append row: %w", err)
					}
				}
			}
		}
	}); err != nil {
		return fmt.Errorf("cw rebuild: append: %w", err)
	}

	if err := execTableDDL(ctx, conn, "BEGIN"); err != nil {
		return fmt.Errorf("cw rebuild: begin swap: %w", err)
	}

	if err := execTableDDL(ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", CaiwuSchema.Name)); err != nil {
		_ = execTableDDL(ctx, conn, "ROLLBACK")
		return fmt.Errorf("cw rebuild: drop target: %w", err)
	}
	if err := execTableDDL(ctx, conn, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", stageName, CaiwuSchema.Name)); err != nil {
		_ = execTableDDL(ctx, conn, "ROLLBACK")
		return fmt.Errorf("cw rebuild: rename stage: %w", err)
	}

	if err := execTableDDL(ctx, conn, "COMMIT"); err != nil {
		return fmt.Errorf("cw rebuild: swap tables: %w", err)
	}

	return nil
}
