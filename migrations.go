package migrations

import (
	"fmt"
	"os"
	"time"
	"database/sql"

	"github.com/Sirupsen/logrus"
	"github.com/altipla-consulting/schema"
	"github.com/altipla-consulting/schema/column"
	"github.com/juju/errors"
)

const tableMigrations = "migrations"

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
	if err := conn.CreateTableIfNotExists(tableMigrations, columns); err != nil {
		return errors.Trace(err)
	}

	collection := dbcollections.Table(tableMigrations)
	stored := []*appliedMigration{}
	if err := collection.OrderBy("runned_at").All(&stored); err != nil {
	  return errors.Trace(err)
	}
	applied := map[string]bool{}
	for _, m := range stored {
		applied[m.Name] = true
	}

	for i, migration := range migrations {
		name := fmt.Sprintf("%03d_%s", i, migration.Name)
		if !applied[name] {
			logrus.WithFields(logrus.Fields{"name": name}).Info("apply migration")
			if err := migration.Apply(db, conn); err != nil {
				return errors.Annotatef(err, "migration %s failed", name)
			}

			if err := collection.Insert(&appliedMigration{Name: name}); err != nil {
			  return errors.Trace(err)
			}
		}
	}

	logrus.Info("migrations applied successfully")
	return nil
}
