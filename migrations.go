package migrations

import (
	"fmt"
	"log"
	"time"

	"github.com/altipla-consulting/schema"
	"github.com/altipla-consulting/schema/column"
	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
)

type M struct {
	Name  string
	Apply func(db *sqlx.DB, conn *schema.Connection) error
}

type AppliedMigration struct {
	Name     string
	RunnedAt time.Time
}

func Run(db *sqlx.DB, migrations []M) error {
	return errors.Trace(RunConnection(db, schema.NewConnection(db.DB), migrations))
}

func RunConnection(db *sqlx.DB, conn *schema.Connection, migrations []M) error {
	log.Println("--- Migrations found:", len(migrations))

	log.Println("--- Check migrations table")
	columns := []schema.Column{
		column.String("name", 191).PrimaryKey(),
		column.DateTime("runned_at").DefaultCurrent(),
	}
	if err := conn.CreateTableIfNotExists("migrations", columns); err != nil {
		return errors.Trace(err)
	}

	stored := []*AppliedMigration{}
	if err := db.Select(&stored, `SELECT * FROM migrations ORDER BY runned_at`); err != nil {
		return errors.Trace(err)
	}
	applied := map[string]bool{}
	for _, m := range stored {
		applied[m.Name] = true
	}

	for i, migration := range migrations {
		name := fmt.Sprintf("%03d_%s", i, migration.Name)
		if !applied[name] {
			log.Println("--- Apply migration:", name)
			if err := migration.Apply(db, conn); err != nil {
				return errors.Annotatef(err, "migration %s failed", name)
			}

			if _, err := db.Exec(`INSERT INTO migrations(name) VALUES (?)`, name); err != nil {
				return errors.Trace(err)
			}
		}
	}

	log.Println("--- Migrations applied successfully!")
	return nil
}
