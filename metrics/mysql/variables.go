package mysql

func (c *Collector) updateVariables(query string, dest map[string]string) error {
	rows, err := c.db.Query(query)
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
