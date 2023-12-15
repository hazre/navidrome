package migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddLibraryTable, downAddLibraryTable)
}

func upAddLibraryTable(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
		create table library (
			id integer primary key autoincrement,
			name text not null unique,
			path text not null unique,
			remote_path text null default '',
			last_scan_at datetime not null default '0000-00-00 00:00:00',
			updated_at datetime not null default current_timestamp,
			created_at datetime not null default current_timestamp
		);`)
	return err
}

func downAddLibraryTable(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `drop table library;`)
	return err
}
