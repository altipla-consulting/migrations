package migrations

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/altipla-consulting/schema"
	"github.com/altipla-consulting/schema/column"
	"github.com/juju/errors"
)

type M struct {
	Name  string
	Apply func(db *sql.DB, conn *schema.Connection) error
}

type appliedMigration struct {
	Name     string
	RunnedAt time.Time
}

func Run(db *sql.DB, migrations []M) error {
	return errors.Trace(RunConnection(db, schema.NewConnection(db), migrations))
}

func RunConnection(db *sql.DB, conn *schema.Connection, migrations []M) error {
	logrus.WithFields(logrus.Fields{"number": len(migrations)}).Info("run migrations")

	columns := []schema.Column{
		column.String("name", 191).PrimaryKey(),
		column.DateTime("runned_at").DefaultCurrent(),
	}
	if err := conn.CreateTableIfNotExists("migrations", columns); err != nil {
		return errors.Trace(err)
	}

	applied := map[string]bool{}
	rows, err := db.Query(`SELECT name FROM migrations ORDER BY runned_at`)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return errors.Trace(err)
		}
		applied[name] = true
	}
	if err := rows.Err(); err != nil {
		return errors.Trace(err)
	}

	for i, migration := range migrations {
		name := fmt.Sprintf("%03d_%s", i, migration.Name)
		if !applied[name] {
			logrus.WithFields(logrus.Fields{"name": name}).Info("apply migration")
			if err := migration.Apply(db, conn); err != nil {
				return errors.Annotatef(err, "migration %s failed", name)
			}

			if _, err := db.Exec(`INSERT INTO migrations(name) VALUES (?)`, name); err != nil {
				return errors.Trace(err)
			}
		}
	}

	logrus.Info("migrations applied successfully")
	return nil
}
