## 3.1.12
  - Fix the Logger initialization in logstash 2.4.X #166

## 3.1.11
  - Uses SO_linger for the socket option to force the server to completely disconnect the idle clients https://github.com/elastic/logstash/issues/6300

## 3.1.10
  - Correctly send the client_inactivity_timeout to the Server classe #163
  - Mark congestion_threshold as deprecated, the java implementation now use a keep alive mechanism

## 3.1.9
  - Docs: Removed statement about intermediate CAs not being supported

## 3.1.8
  - Fix a typo in the default ciphers suite, added validations for the configured ciphers #156
  - validate the presence of `ssl_certificate_authorities` when `verify_mode` is set to FORCE_PEER or peer #155

## 3.1.7
  - Fix an issue when only the first CA found in the certificate authorities was taking into consideration to verify clients #153

## 3.1.6
   - Fix an issue with the `READER_IDLE` that was closing a connection in a middle of working on a batch #141
   - Fix an issue when the plugin did not accept a specific host to bind to. #146
   - Fix an issue when forcing a logstash shutdown that could result in an `InterruptedException` #145

## 3.1.5
   - Fix an issue when using a passphrase was raising a TypeError #138
   - Fix the filebeat integration suite to use the new `ssl` option instead of `tls`
   - Use correct log4j logger call to be compatible with 2.4

## 3.1.4
   - Add a note concerning the requirement of the PKCS8 format for the private key.

## 3.1.3
   - Use a relative path for the VERSION, this change is needed by the doc generation tool to read the gemspec.

## 3.1.2
   - Propagate SSL handshake error correctly
   - Move back to log4j 1, to make it work better under logstash 2.4

## 3.1.1
   - Remove the SSL Converter, Private Key must be in the PKCS8 format, which is the default of any newer OpenSSL library
   - Replace FileInputStream with File reference to let netty handle correctly the certificates
   - Tests now uses OpenSSL binary to convert PKCS7 Private generated from ruby to PKCS8
   - Remove dependency on bouncycastle
   - Fix an issue when the input could hang forever when stopping Logstash
   - [Doc changes] Add Logstash config example and clarify use of the `type` config option

## 3.1.0
   - Fix a NullPointer Exception https://github.com/elastic/logstash/issues/5756
   - Log4j ERROR will now be propagated upstream, IE: InvalidCertificate OR InvalidFrameType.
   - Relax constraints on multiline to make it work under 2.4

## 3.1.0.beta4
    - Fix a problem that would would make the server refuse concurrent connection to the server #111

## 3.1.0.beta3
    - Jars were missing from the latest release on rubygems

## 3.1.0.beta2
    - Better handling of connection timeout, added a new option to set the value for it, the default is 15 seconds #108
    - Make sure that incomplete SSL handshake doesn't take down the server #101
    - Sending Garbage data will now raise a specific exception `InvalidFrameProtocolException` #100
    - Adding assertions on the payload size and the fields count to make the parser more resilient to erronous frames #99

## 3.1.0.beta1
    - Rewrite of the beats input in Java using the Netty framewwork, this rewrite is meant to be backward compatible with the previous implementation

## 3.0.4
  - Relax constraint on logstash-core-plugin-api to >= 1.60 <= 2.99
    but should yield better throughput and memory usage. https://github.com/logstash-plugins/logstash-input-beats/pull/93

## 3.0.3

  - Fix an issue when parsing multiple frames received from a filebeat client using pipelining.

## 3.0.2

  - relax constrains of `logstash-devutils` see https://github.com/elastic/logstash-devutils/issues/48

## 3.0.1

  - Republish all the gems under jruby.

## 3.0.0

  - Update the plugin to the version 2.0 of the plugin api, this change is required for Logstash 5.0 compatibility. See https://github.com/elastic/logstash/issues/5141

## 2.2.8

  - Fix #73 Bug in EventTransformCommon#codec_name, use config_name
  - Add regression test for fix to #73
  - Non deterministic error for the LSF integration test
  - Make this plugin really a drop in replacement for the lumberjack input, so LSF can send their events to this plugin.

## 2.2.7

  - More robust test when using a random port #60
  - Fix LSF integration tests #52

## 2.2.6

  - Do not use the identity map if we don't explicitly use the `multiline` codec

## 2.2.5

  - Fix failing tests introduce by the `ssl_key_passphrase` changes.
  - Added an integration test for the `ssl_key_passphrase`
  - Add an optional parameter for `auto_flush`

## 2.2.4

  - Fix bug where using `ssl_key_passphrase` wouldn't work

## 2.2.2

  - Depend on logstash-core-plugin-api instead of logstash-core, removing the need to mass update plugins on major releases of logstash

## 2.2.1

  - New dependency requirements for logstash-core for the 5.0 release

## 2.2.0

  - The server can now do client side verification by providing a list of certificate authorities and configuring the `ssl_verify_mode`,
    the server can use `peer`, if the client send a certificate it will be validated. Using `force_peer` will make sure the client provide a certificate
    and it will be validated with the know CA.  #8

## 2.1.4

  - Change the `logger#warn` for `logger.debug` when a peer get disconnected, keep alive check from proxy can generate a lot of logs  #46

## 2.1.3

  - Make sure we stop all the threads after running the tests #48

## 2.1.2

  - Catch the `java.lang.InterruptedException` in the events broker
  - Give a bit more time to the Thread to be started in the test #42

## 2.1.1

  - Release a new version of the gem that doesn't included any other gems, 2.1.0 is yanked from rubygems

## 2.1.0

  - Refactor of the code to make it easier to unit test
  - Fix a conncurrency error on high load on the SizeQueue #37
  - Drop the internal SizeQueue to rely on Java Synchronous Queue
  - Remove the majority of the nested blocks
  - Move the CircuitBreaker inside an internal namespace so it doesn't conflict with the input lumberjack
  - Add more debugging log statement
  - Flush the codec when a disconnect happen
  - Tag/Decorate the event when a shutdown occur.
  - The name of the threads managed by the input beat are now meaningful.

## 2.0.3

  - Reduce the size of the gem by removing vendor jars

## 2.0.2

  - Copy the `beat.hostname` field into the `host` field for better compatibility with the other Logstash plugins #28
  - Correctly merge multiple line with the multiline codec ref: #24

## 2.0.0

  - Add support for stream identity, the ID will be generated from beat.id+resource_id or beat.name + beat.source if not present #22 #13
    The identity allow the multiline codec to correctly merge string from multiples files.

## 0.9.6

  - Fix an issue with rogue events created by buffered codecs #19

## 0.9.5

  - Set concurrent-ruby to 0.9.1 see https://github.com/elastic/logstash/issues/4141

## 0.9.4

  - Correctly decorate the event with the `add_field` and `tags` option from the config #12

## 0.9.3

  - Connection#run should rescue `Broken Pipe Error` #5
  - Fix a `SystemCallErr` issue on windows when shutting down the server #9

## 0.9.2

  - fix an issue with the incorrectly calculated ack when the window_size was smaller than the ACK_RATIO see  https://github.com/logstash-plugins/logstash-input-beats/issues/3

## 0.9.1

  - Move the ruby-lumberjack library into the plugin

## 0.9
  - Created from `logstash-input-lumberjack` version 2.0.2 https://github.com/logstash-plugins/logstash-input-lumberjack
  - Use SSL off by default
