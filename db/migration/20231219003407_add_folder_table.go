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

alter table media_file 
    add column folder_id varchar default "" not null;
alter table media_file 
    add column pid varchar default id not null;
alter table media_file 
    add column album_pid varchar default album_id not null;

create index if not exists media_file_folder_id_index
 	on media_file (folder_id);
create index if not exists media_file_pid_index
	on media_file (pid);
create index if not exists media_file_album_pid_index
	on media_file (album_pid);

-- FIXME Needs to process current media_file.paths, creating folders as needed
`)

	return err
}

func downAddFolderTable(ctx context.Context, tx *sql.Tx) error {
	// This code is executed when the migration is rolled back.
	return nil
}
