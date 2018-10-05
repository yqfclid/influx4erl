-record(influx_conf, {host,
					  port,
					  username,
					  password,
					  database,
					  protocol = http,
					  epoch,
					  name,
					  http_pool = default
					  }).