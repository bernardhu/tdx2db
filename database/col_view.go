package database

type ColumnView struct {
	name  string
	alias string
	desc  string
}

type ColumnViews struct {
	name   string
	fields []ColumnView
	from   string
	desc   string
}
