package mysql

import "context"

func (c *Collector) updateVariables(ctx context.Context, query string, dest map[string]string) error {
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	clear(dest)
	defer rows.Close()
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			c.logger.Warning(err)
			continue
		}
		dest[name] = value
	}
	return nil
}
