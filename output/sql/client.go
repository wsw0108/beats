package sql

import (
	"bytes"
	"database/sql"
	"time"

	_ "github.com/mattn/go-sqlite3"

	_ "github.com/denisenkom/go-mssqldb"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/outputs"
)

type client struct {
	config *sqlConfig
	db     *sql.DB
}

func newClient(config *sqlConfig) (*client, error) {
	return &client{
		config: config,
	}, nil
}

func (c *client) Connect(timeout time.Duration) error {
	driver := c.config.Driver
	dsn := c.config.DSN
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return err
	}
	c.db = db
	return c.db.Ping()
}

func (c *client) Close() error {
	return c.db.Close()
}

func (c *client) PublishEvent(data outputs.Data) error {
	event := data.Event
	len := len(event)
	names := make([]string, 0, len)
	values := make([]interface{}, 0, len)
	for name, value := range event {
		if name == "@timestamp" {
			name = "timestamp"
		}
		switch v := value.(type) {
		case common.Time:
			value = v.String()
		case common.MapStr:
			value = v.String()
		}
		names = append(names, name)
		values = append(values, value)
	}
	var b bytes.Buffer
	b.WriteString("INSERT INTO ")
	b.WriteString(c.config.Table)
	b.WriteByte(' ')
	b.WriteByte('(')
	for i, name := range names {
		b.WriteString(name)
		if i < len-1 {
			b.WriteString(", ")
		}
	}
	b.WriteByte(')')
	b.WriteString(" VALUES ")
	b.WriteByte('(')
	for i := range values {
		b.WriteString("?")
		if i < len-1 {
			b.WriteString(", ")
		}
	}
	b.WriteByte(')')
	b.WriteByte(';')
	query := b.String()
	debugf("query: '%s'", query)
	_, err := c.db.Exec(query, values...)
	return err
}

func (c *client) PublishEvents(data []outputs.Data) ([]outputs.Data, error) {
	for i, d := range data {
		err := c.PublishEvent(d)
		if err != nil {
			return data[i:], err
		}
	}
	return nil, nil
}
