package syncer

import (
	"fmt"
	"regexp"
	"strings"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*$`)

type tableName struct {
	Schema string
	Name   string
}

func parseTableName(value string) (tableName, error) {
	parts := strings.Split(value, ".")
	if len(parts) == 1 {
		parts = []string{"public", parts[0]}
	}
	if len(parts) != 2 || !validIdentifier(parts[0]) || !validIdentifier(parts[1]) {
		return tableName{}, fmt.Errorf("无效表名 %q，应为 schema.table", value)
	}
	return tableName{Schema: parts[0], Name: parts[1]}, nil
}

func validIdentifier(value string) bool { return identifierPattern.MatchString(value) }

func quoteIdentifier(value string) string { return `"` + strings.ReplaceAll(value, `"`, `""`) + `"` }

func (t tableName) SQL() string { return quoteIdentifier(t.Schema) + "." + quoteIdentifier(t.Name) }
