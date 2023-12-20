package migrations

import (
	"context"
	"database/sql"

	"github.com/pressly/goose/v3"
)

func init() {
	goose.AddMigrationContext(upAddFolderTable, downAddFolderTable)
}

func upAddFolderTable(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
create table if not exists folder(
	id varchar not null
		primary key,
	library_id integer not null
	    		references library (id)
	    		 	on delete cascade,
	path varchar default '' not null,
	name varchar default '' not null,
	updated_at timestamp default current_timestamp not null,
	created_at timestamp default current_timestamp not null,
	parent_id varchar default null
		references folder (id)
		 	on delete cascade
);
`)

	return err
}

func downAddFolderTable(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	return nil
}
