package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/tdx"
)

type GpWideRow struct {
	RDate   time.Time
	Values  []float32
	Present []uint64
}

type GpWideBatch struct {
	Code string
	Mkt  string
	Rows []GpWideRow
}

type gpFieldIndex struct {
	idx0 int
	idx1 int
}

var gpFieldCount, gpFieldWords, gpFieldIndexLookup = initGpFieldMeta()

func initGpFieldMeta() (count int, words int, lookup map[byte]gpFieldIndex) {
	idx := 0
	lookup = make(map[byte]gpFieldIndex, len(gpbase))
	for _, desc := range gpbase {
		fi := gpFieldIndex{idx0: idx, idx1: -1}
		idx++
		if desc.name1 != "" {
			fi.idx1 = idx
			idx++
		}
		lookup[desc.typ] = fi
	}

	count = idx
	words = (count + 63) / 64
	return count, words, lookup
}

func setPresent(mask []uint64, idx int) {
	word := idx / 64
	bit := uint(idx % 64)
	mask[word] |= 1 << bit
}

func isPresent(mask []uint64, idx int) bool {
	word := idx / 64
	bit := uint(idx % 64)
	return mask[word]&(1<<bit) != 0
}

func AggregateGpRecords(recs []tdx.GpRecord) (GpWideBatch, error) {
	var batch GpWideBatch
	if len(recs) == 0 {
		return batch, nil
	}

	batch.Code = recs[0].Code
	batch.Mkt = recs[0].Mkt

	type aggRow struct {
		values  []float32
		present []uint64
	}

	byDate := make(map[uint32]*aggRow, 1024)

	for _, record := range recs {
		fi, ok := gpFieldIndexLookup[record.RecType]
		if !ok {
			continue
		}

		key := record.ReportDate
		if key == 0 && record.RecType == 10 {
			now := time.Now()
			key = uint32(now.Year()*10000) + uint32(now.Month()*100) + uint32(now.Day())
		}

		if fixDay, fix := fixDate(key); fix {
			key = fixDay
		}
		if key == 0 {
			continue
		}

		row := byDate[key]
		if row == nil {
			row = &aggRow{
				values:  make([]float32, gpFieldCount),
				present: make([]uint64, gpFieldWords),
			}
			byDate[key] = row
		}

		row.values[fi.idx0] = record.Val1
		setPresent(row.present, fi.idx0)
		if fi.idx1 >= 0 {
			row.values[fi.idx1] = record.Val2
			setPresent(row.present, fi.idx1)
		}
	}

	keys := make([]uint32, 0, len(byDate))
	for k := range byDate {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	batch.Rows = make([]GpWideRow, 0, len(keys))
	for _, k := range keys {
		t, err := parseReportDate(k)
		if err != nil {
			return GpWideBatch{}, fmt.Errorf("invalid report date %d: %w", k, err)
		}
		r := byDate[k]
		batch.Rows = append(batch.Rows, GpWideRow{
			RDate:   t,
			Values:  r.values,
			Present: r.present,
		})
	}

	return batch, nil
}

func RebuildGpBase(ctx context.Context, db *sql.DB, batches <-chan GpWideBatch) error {
	targetTable := GpSchema.Name
	stageTable := targetTable + "_stage"

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("gp rebuild: get conn: %w", err)
	}
	defer conn.Close()

	stageSchema := TableSchema{
		Name:    stageTable,
		Columns: append([]string(nil), GpSchema.Columns...),
		Keys:    append([]string(nil), GpSchema.Keys...),
	}

	if err := execTableDDL(ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", stageSchema.Name)); err != nil {
		return fmt.Errorf("gp rebuild: drop stage: %w", err)
	}

	if err := createTableOnConn(ctx, conn, stageSchema); err != nil {
		return fmt.Errorf("gp rebuild: create stage: %w", err)
	}

	columnCount := len(stageSchema.Columns)
	fieldOffset := 3
	fieldCount := columnCount - fieldOffset

	if err := conn.Raw(func(dc any) error {
		driverConn, ok := dc.(driver.Conn)
		if !ok {
			return fmt.Errorf("gp rebuild: unexpected driver conn type %T", dc)
		}

		appender, err := duckdb.NewAppenderFromConn(driverConn, "", stageTable)
		if err != nil {
			return fmt.Errorf("gp rebuild: new appender: %w", err)
		}
		closed := false
		defer func() {
			if !closed {
				_ = appender.Close()
			}
		}()

		rowValues := make([]driver.Value, columnCount)

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case batch, ok := <-batches:
				if !ok {
					if err := appender.Close(); err != nil {
						return fmt.Errorf("gp rebuild: close appender: %w", err)
					}
					closed = true
					return nil
				}

				rowValues[0] = batch.Code
				rowValues[1] = batch.Mkt

				for _, row := range batch.Rows {
					if len(row.Values) != gpFieldCount || len(row.Present) != gpFieldWords {
						return fmt.Errorf("gp rebuild: unexpected gp row shape values=%d present=%d", len(row.Values), len(row.Present))
					}

					rowValues[2] = row.RDate

					for i := 0; i < fieldCount; i++ {
						if isPresent(row.Present, i) {
							rowValues[fieldOffset+i] = float64(row.Values[i])
						} else {
							rowValues[fieldOffset+i] = nil
						}
					}

					if err := appender.AppendRow(rowValues...); err != nil {
						return fmt.Errorf("gp rebuild: append row: %w", err)
					}
				}
			}
		}
	}); err != nil {
		return fmt.Errorf("gp rebuild: append: %w", err)
	}

	if err := execTableDDL(ctx, conn, "BEGIN"); err != nil {
		return fmt.Errorf("gp rebuild: begin swap: %w", err)
	}
	if err := execTableDDL(ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", targetTable)); err != nil {
		_ = execTableDDL(ctx, conn, "ROLLBACK")
		return fmt.Errorf("gp rebuild: drop target: %w", err)
	}
	if err := execTableDDL(ctx, conn, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", stageTable, targetTable)); err != nil {
		_ = execTableDDL(ctx, conn, "ROLLBACK")
		return fmt.Errorf("gp rebuild: rename stage: %w", err)
	}
	if err := execTableDDL(ctx, conn, "COMMIT"); err != nil {
		return fmt.Errorf("gp rebuild: swap tables: %w", err)
	}

	return nil
}

func execTableDDL(ctx context.Context, conn *sql.Conn, query string) error {
	_, err := conn.ExecContext(ctx, query)
	return err
}

func createTableOnConn(ctx context.Context, conn *sql.Conn, schema TableSchema) error {
	columnsStr := strings.Join(schema.Columns, ", ")
	if len(schema.Keys) > 0 {
		columnsStr = columnsStr + ", " + strings.Join(schema.Keys, ", ")
	}
	query := fmt.Sprintf(`CREATE TABLE %s (%s)`, schema.Name, columnsStr)

	_, err := conn.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("create table %s: %w qry:%s", schema.Name, err, query)
	}
	return nil
}
