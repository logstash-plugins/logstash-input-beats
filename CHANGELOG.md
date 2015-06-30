# 1.0.2
  - Use a `require_relative` for loading the spec helpers. (https://github.com/logstash-plugins/logstash-input-lumberjack/pull/35)
# 1.0.1
  - Fix a randomization issue for rspec https://github.com/logstash-plugins/logstash-input-lumberjack/issues/33
# 1.0.0
  - Default supported plugins are pushed to 1.0
# 0.1.10
  - Deprecating the `max_clients` option
  - Use a circuit breaker to start refusing new connection when the queue is blocked for too long.
  - Add an internal `SizeQueue` with a timeout to drop blocked connections. (https://github.com/logstash-plugins/logstash-input-lumberjack/pull/12)
