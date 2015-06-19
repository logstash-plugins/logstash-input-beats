# 0.1.10
  - Deprecating the `max_clients` option
  - Use a circuit breaker to start refusing new connection when the queue is blocked for too long.
  - Add an internal `SizeQueue` with a timeout to drop blocked connections. (https://github.com/logstash-plugins/logstash-input-lumberjack/pull/12)
