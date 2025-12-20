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

type gpFieldMeta struct {
	count  int
	words  int
	lookup map[byte]gpFieldIndex
}

func initGpFieldMeta(descs []gpColumnDesc) gpFieldMeta {
	idx := 0
	lookup := make(map[byte]gpFieldIndex, len(descs))
	for _, desc := range descs {
		fi := gpFieldIndex{idx0: idx, idx1: -1}
		idx++
		if desc.name1 != "" {
			fi.idx1 = idx
			idx++
		}
		lookup[desc.typ] = fi
	}

	count := idx
	words := (count + 63) / 64
	return gpFieldMeta{
		count:  count,
		words:  words,
		lookup: lookup,
	}
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

var (
	gpBaseFieldMeta = initGpFieldMeta(gpbase)
	gpBlkFieldMeta  = initGpFieldMeta(blkbase)
	gpMktFieldMeta  = initGpFieldMeta(mktbase)
)

func AggregateGpBatches(recs []tdx.GpRecord) ([]GpWideBatch, error) {
	return aggregateGpRecords(recs, gpBaseFieldMeta)
}

func AggregateGpRecords(recs []tdx.GpRecord) (GpWideBatch, error) {
	batches, err := AggregateGpBatches(recs)
	if err != nil {
		return GpWideBatch{}, err
	}
	if len(batches) == 0 {
		return GpWideBatch{}, nil
	}
	if len(batches) > 1 {
		return GpWideBatch{}, fmt.Errorf("aggregate gp records: expected 1 batch, got %d", len(batches))
	}
	return batches[0], nil
}

func AggregateBlkBatches(recs []tdx.GpRecord) ([]GpWideBatch, error) {
	return aggregateGpRecords(recs, gpBlkFieldMeta)
}

func AggregateBlkRecords(recs []tdx.GpRecord) (GpWideBatch, error) {
	batches, err := AggregateBlkBatches(recs)
	if err != nil {
		return GpWideBatch{}, err
	}
	if len(batches) == 0 {
		return GpWideBatch{}, nil
	}
	if len(batches) > 1 {
		return GpWideBatch{}, fmt.Errorf("aggregate blk records: expected 1 batch, got %d", len(batches))
	}
	return batches[0], nil
}

func AggregateMktBatches(recs []tdx.GpRecord) ([]GpWideBatch, error) {
	return aggregateGpRecords(recs, gpMktFieldMeta)
}

func AggregateMktRecords(recs []tdx.GpRecord) (GpWideBatch, error) {
	batches, err := AggregateMktBatches(recs)
	if err != nil {
		return GpWideBatch{}, err
	}
	if len(batches) == 0 {
		return GpWideBatch{}, nil
	}
	if len(batches) > 1 {
		return GpWideBatch{}, fmt.Errorf("aggregate mkt records: expected 1 batch, got %d", len(batches))
	}
	return batches[0], nil
}

func aggregateGpRecords(recs []tdx.GpRecord, meta gpFieldMeta) ([]GpWideBatch, error) {
	if len(recs) == 0 {
		return nil, nil
	}

	type aggRow struct {
		values  []float32
		present []uint64
	}

	type batchKey struct {
		code string
		mkt  string
	}

	byBatch := make(map[batchKey]map[uint32]*aggRow, 16)

	for _, record := range recs {
		fi, ok := meta.lookup[record.RecType]
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

		k := batchKey{code: record.Code, mkt: record.Mkt}
		byDate := byBatch[k]
		if byDate == nil {
			byDate = make(map[uint32]*aggRow, 1024)
			byBatch[k] = byDate
		}

		row := byDate[key]
		if row == nil {
			row = &aggRow{
				values:  make([]float32, meta.count),
				present: make([]uint64, meta.words),
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

	if len(byBatch) == 0 {
		return nil, nil
	}

	batchKeys := make([]batchKey, 0, len(byBatch))
	for k := range byBatch {
		batchKeys = append(batchKeys, k)
	}
	sort.Slice(batchKeys, func(i, j int) bool {
		if batchKeys[i].code != batchKeys[j].code {
			return batchKeys[i].code < batchKeys[j].code
		}
		return batchKeys[i].mkt < batchKeys[j].mkt
	})

	batches := make([]GpWideBatch, 0, len(batchKeys))
	for _, bk := range batchKeys {
		byDate := byBatch[bk]

		keys := make([]uint32, 0, len(byDate))
		for k := range byDate {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

		batch := GpWideBatch{
			Code: bk.code,
			Mkt:  bk.mkt,
			Rows: make([]GpWideRow, 0, len(keys)),
		}
		for _, k := range keys {
			t, err := parseReportDate(k)
			if err != nil {
				return nil, fmt.Errorf("invalid report date %d: %w", k, err)
			}
			r := byDate[k]
			batch.Rows = append(batch.Rows, GpWideRow{
				RDate:   t,
				Values:  r.values,
				Present: r.present,
			})
		}

		batches = append(batches, batch)
	}

	return batches, nil
}

type GpRebuildKind uint8

const (
	GpRebuildBase GpRebuildKind = iota
	GpRebuildBlk
	GpRebuildMkt
)

type GpRebuildBatch struct {
	Kind  GpRebuildKind
	Batch GpWideBatch
}

func RebuildGpTables(ctx context.Context, db *sql.DB, rebuildBase, rebuildBlk, rebuildMkt bool, batches <-chan GpRebuildBatch) error {
	type rebuildPlan struct {
		label      string
		targetName string
		stageName  string
		stage      TableSchema
		meta       gpFieldMeta
		includeMkt bool
	}

	plans := make(map[GpRebuildKind]rebuildPlan, 3)

	if rebuildBase {
		stageName := GpSchema.Name + "_stage"
		plans[GpRebuildBase] = rebuildPlan{
			label:      "gp base",
			targetName: GpSchema.Name,
			stageName:  stageName,
			stage: TableSchema{
				Name:    stageName,
				Columns: append([]string(nil), GpSchema.Columns...),
				Keys:    append([]string(nil), GpSchema.Keys...),
			},
			meta:       gpBaseFieldMeta,
			includeMkt: true,
		}
	}
	if rebuildBlk {
		stageName := BlkSchema.Name + "_stage"
		plans[GpRebuildBlk] = rebuildPlan{
			label:      "gp blk",
			targetName: BlkSchema.Name,
			stageName:  stageName,
			stage: TableSchema{
				Name:    stageName,
				Columns: append([]string(nil), BlkSchema.Columns...),
				Keys:    append([]string(nil), BlkSchema.Keys...),
			},
			meta:       gpBlkFieldMeta,
			includeMkt: false,
		}
	}
	if rebuildMkt {
		stageName := MktSchema.Name + "_stage"
		plans[GpRebuildMkt] = rebuildPlan{
			label:      "gp mkt",
			targetName: MktSchema.Name,
			stageName:  stageName,
			stage: TableSchema{
				Name:    stageName,
				Columns: append([]string(nil), MktSchema.Columns...),
				Keys:    append([]string(nil), MktSchema.Keys...),
			},
			meta:       gpMktFieldMeta,
			includeMkt: false,
		}
	}

	if len(plans) == 0 {
		return nil
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("gp rebuild: get conn: %w", err)
	}
	defer conn.Close()

	for _, plan := range plans {
		if err := execTableDDL(ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", plan.stageName)); err != nil {
			return fmt.Errorf("%s rebuild: drop stage: %w", plan.label, err)
		}
		if err := createTableOnConn(ctx, conn, plan.stage); err != nil {
			return fmt.Errorf("%s rebuild: create stage: %w", plan.label, err)
		}
	}

	type tableState struct {
		plan       rebuildPlan
		appender   *duckdb.Appender
		rowValues  []driver.Value
		fieldCount int
	}

	if err := conn.Raw(func(dc any) error {
		driverConn, ok := dc.(driver.Conn)
		if !ok {
			return fmt.Errorf("gp rebuild: unexpected driver conn type %T", dc)
		}

		stateByKind := make(map[GpRebuildKind]*tableState, len(plans))
		closed := make(map[GpRebuildKind]bool, len(plans))
		defer func() {
			for kind, st := range stateByKind {
				if closed[kind] {
					continue
				}
				_ = st.appender.Close()
			}
		}()

		for kind, plan := range plans {
			appender, err := duckdb.NewAppenderFromConn(driverConn, "", plan.stageName)
			if err != nil {
				return fmt.Errorf("%s rebuild: new appender: %w", plan.label, err)
			}

			columnCount := len(plan.stage.Columns)
			fieldOffset := 3
			fieldCount := columnCount - fieldOffset
			if fieldCount != plan.meta.count {
				return fmt.Errorf("%s rebuild: schema mismatch: table fields=%d meta fields=%d", plan.label, fieldCount, plan.meta.count)
			}

			stateByKind[kind] = &tableState{
				plan:       plan,
				appender:   appender,
				rowValues:  make([]driver.Value, columnCount),
				fieldCount: fieldCount,
			}
		}

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case item, ok := <-batches:
				if !ok {
					for kind, st := range stateByKind {
						if err := st.appender.Close(); err != nil {
							return fmt.Errorf("%s rebuild: close appender: %w", st.plan.label, err)
						}
						closed[kind] = true
					}
					return nil
				}

				st := stateByKind[item.Kind]
				if st == nil {
					return fmt.Errorf("gp rebuild: unexpected batch kind %d", item.Kind)
				}

				rowValues := st.rowValues
				rowValues[0] = item.Batch.Code
				if st.plan.includeMkt {
					rowValues[1] = item.Batch.Mkt
				} else {
					rowValues[1] = nil
				}

				for _, row := range item.Batch.Rows {
					if len(row.Values) != st.plan.meta.count || len(row.Present) != st.plan.meta.words {
						return fmt.Errorf("%s rebuild: unexpected row shape values=%d present=%d", st.plan.label, len(row.Values), len(row.Present))
					}

					rowValues[2] = row.RDate

					for i := 0; i < st.fieldCount; i++ {
						if isPresent(row.Present, i) {
							rowValues[3+i] = float64(row.Values[i])
						} else {
							rowValues[3+i] = nil
						}
					}

					if err := st.appender.AppendRow(rowValues...); err != nil {
						return fmt.Errorf("%s rebuild: append row: %w", st.plan.label, err)
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

	for _, plan := range plans {
		if err := execTableDDL(ctx, conn, fmt.Sprintf("DROP TABLE IF EXISTS %s", plan.targetName)); err != nil {
			_ = execTableDDL(ctx, conn, "ROLLBACK")
			return fmt.Errorf("%s rebuild: drop target: %w", plan.label, err)
		}
		if err := execTableDDL(ctx, conn, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", plan.stageName, plan.targetName)); err != nil {
			_ = execTableDDL(ctx, conn, "ROLLBACK")
			return fmt.Errorf("%s rebuild: rename stage: %w", plan.label, err)
		}
	}

	if err := execTableDDL(ctx, conn, "COMMIT"); err != nil {
		return fmt.Errorf("gp rebuild: swap tables: %w", err)
	}

	return nil
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
					if len(row.Values) != gpBaseFieldMeta.count || len(row.Present) != gpBaseFieldMeta.words {
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
