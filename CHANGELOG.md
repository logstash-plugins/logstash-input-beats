## 6.8.3
  - bump netty to 4.1.109 [#495](https://github.com/logstash-plugins/logstash-input-beats/pull/495)

## 6.8.2
  - Remove Logstash forwarder test cases and add Lumberjack test cases [#488](https://github.com/logstash-plugins/logstash-input-beats/pull/488)

## 6.8.1
  - Added logging the best Netty's `maxOrder` setting when big payload trigger an unpooled allocation [#493](https://github.com/logstash-plugins/logstash-input-beats/pull/493)

## 6.8.0
  - Introduce expert only `event_loop_threads` to tune netty event loop threads count [#490](https://github.com/logstash-plugins/logstash-input-beats/pull/490)

## 6.7.2
  - Restore accidentally removed Lumberjack event parse source lines [#486](https://github.com/logstash-plugins/logstash-input-beats/pull/486)

## 6.7.1
  - bump netty to 4.1.100 and jackson to 2.15.3 [#484](https://github.com/logstash-plugins/logstash-input-beats/pull/484)

## 6.7.0
 - PROTOCOL: adds explicit support for receiving a 0-length window to encapsulate an empty batch. Empty batches are acknowledged with the same 0-sequence ACK's that are used as keep-alives during processing. [#479](https://github.com/logstash-plugins/logstash-input-beats/pull/479)  

## 6.6.4
 - [DOC] Fix misleading `enrich/source_data` input beats documentation about the Logstash host. [#478](https://github.com/logstash-plugins/logstash-input-beats/pull/478)

## 6.6.3
 - [DOC] Updated the `ssl_client_authentication` and `ssl_verify_mode` documentation explaining that CN and SAN are not validated. [#473](https://github.com/logstash-plugins/logstash-input-beats/pull/473)

## 6.6.2
  - bump netty to 4.1.94 and jackson to 2.15.2 [#474](https://github.com/logstash-plugins/logstash-input-beats/pull/474)

## 6.6.1
  - update netty to 4.1.93 and jackson to 2.13.5 [#472](https://github.com/logstash-plugins/logstash-input-beats/pull/472)

## 6.6.0
 - Reviewed and deprecated SSL settings to comply with Logstash's naming convention [#470](https://github.com/logstash-plugins/logstash-input-beats/pull/470)
   - Deprecated `ssl` in favor of `ssl_enabled`
   - Deprecated `ssl_verify_mode` in favor of `ssl_client_authentication`

## 6.5.0
 - An enrichment `enrich` option added to control ECS passthrough. `ssl_peer_metadata` and `include_codec_tag` configurations are deprecated and can be managed through the `enrich`  [#464](https://github.com/logstash-plugins/logstash-input-beats/pull/464)

## 6.4.4
 - Updates Netty dependency to 4.1.87 [#466](https://github.com/logstash-plugins/logstash-input-beats/pull/466)

## 6.4.3
  - [DOC] `executor_threads` default value explanation updated. [#461](https://github.com/logstash-plugins/logstash-input-beats/pull/461)

## 6.4.2
  - Build: do not package jackson dependencies [#455](https://github.com/logstash-plugins/logstash-input-beats/pull/455)

## 6.4.1
  - [DOC] Add direct memory example [#454](https://github.com/logstash-plugins/logstash-input-beats/pull/454)

## 6.4.0
 - Feat: review and deprecate ssl protocol/cipher settings [#450](https://github.com/logstash-plugins/logstash-input-beats/pull/450)

## 6.3.1
 - Fix: Removed use of deprecated `import` of java classes in ruby [#449](https://github.com/logstash-plugins/logstash-input-beats/pull/449)

## 6.3.0
 - Added support for TLSv1.3. [#447](https://github.com/logstash-plugins/logstash-input-beats/pull/447)

## 6.2.6
  - Update guidance regarding the private key format and encoding [#445](https://github.com/logstash-plugins/logstash-input-beats/pull/445)

## 6.2.5
 - Build: do not package log4j-api dependency [#441](https://github.com/logstash-plugins/logstash-input-beats/pull/441).
   Logstash provides the log4j framework and the dependency is not needed except testing and compiling.

## 6.2.4
 - Updated log4j dependency to 2.17.0

## 6.2.3
 - Updated log4j dependency to 2.15.0

## 6.2.2
 - Fix: update to Gradle 7 [#432](https://github.com/logstash-plugins/logstash-input-beats/pull/432)
 - [DOC] Edit documentation for `executor_threads` [#435](https://github.com/logstash-plugins/logstash-input-beats/pull/435)

## 6.2.1
 - Fix: LS failing with `ssl_peer_metadata => true` [#431](https://github.com/logstash-plugins/logstash-input-beats/pull/431)
 - [DOC] described `executor_threads` configuration parameter [#421](https://github.com/logstash-plugins/logstash-input-beats/pull/421)

## 6.2.0
 - ECS compatibility enablement: Adds alias to support upcoming ECS v8 with the existing ECS v1 implementation

## 6.1.7
 - [DOC] Remove limitations topic and link [#428](https://github.com/logstash-plugins/logstash-input-beats/pull/428)

## 6.1.6
 - [DOC] Applied more attributes to manage plugin name in doc content, and implemented conditional text processing. [#423](https://github.com/logstash-plugins/logstash-input-http/pull/423)

## 6.1.5
 - Changed jar dependencies to reflect newer versions [#425](https://github.com/logstash-plugins/logstash-input-beats/pull/425)

## 6.1.4
 - Fix: reduce error logging on connection resets [#424](https://github.com/logstash-plugins/logstash-input-beats/pull/424)

## 6.1.3
 - Fix: safe-guard byte buf allocation [#420](https://github.com/logstash-plugins/logstash-input-beats/pull/420) 
 - Updated Jackson dependencies

## 6.1.2
 - [DOC] Added naming attribute to control plugin name that appears in docs, and set up framework to make attributes viable in code sample

## 6.1.1
 - [DOC] Enhanced ECS compatibility information for ease of use and readability
 [#413](https://github.com/logstash-plugins/logstash-input-beats/pull/413)

## 6.1.0
 - ECS compatibility enablement. Adds `ecs_compatibility` setting to declare the level of ECS compatibility (`disabled` or `v1`) at plugin level. When `disabled`, the plugin behaves like before, while `v1` does a rename of
   `host` and `@metadata.ip_address` event fields. [#404](https://github.com/logstash-plugins/logstash-input-beats/pull/404)

## 6.0.14
 - Feat: log + unwrap generic SSL context exceptions [#405](https://github.com/logstash-plugins/logstash-input-beats/pull/405)

## 6.0.13
 - [DOC] Update links to use shared attributes

## 6.0.12
 - Fix: log error when SSL context building fails [#402](https://github.com/logstash-plugins/logstash-input-beats/pull/402).
   We've also made sure to log messages on configuration errors as LS 7.8/7.9 only prints details when level set to debug.

## 6.0.11
 - Updated jackson databind and Netty dependencies. Additionally, this release removes the dependency on `tcnative` +
      `boringssl`, using JVM supplied ciphers instead. This may result in fewer ciphers being available if the JCE
      unlimited strength jurisdiction policy is not installed. (This policy is installed by default on versions of the
      JDK from u161 onwards)[#393](https://github.com/logstash-plugins/logstash-input-beats/pull/393)

## 6.0.10
 - Added error handling to detect if ssl certificate or key files can't be read [#394](https://github.com/logstash-plugins/logstash-input-beats/pull/394)

## 6.0.9
 - Fixed issue where calling `java_import` on `org.logstash.netty.SslContextBuilder` was causing the TCP input to pick up the wrong SslContextBuilder class
   potentially causing pipeline creation to fail [#388](https://github.com/logstash-plugins/logstash-input-beats/pull/388)

## 6.0.8
 - Fixed issue where an SslContext was unnecessarily being created for each connection [#383](https://github.com/logstash-plugins/logstash-input-beats/pull/383)
 - Fixed issue where `end` was not being called when an Inflater was closed [#383](https://github.com/logstash-plugins/logstash-input-beats/pull/383)

## 6.0.7
 - Reverted changes to netty and tcnative dependencies and removal of CBC ciphers to preserve compatibility with Lumberjack output [#381](https://github.com/logstash-plugins/logstash-input-beats/pull/381)

## 6.0.6
 - Downgraded netty to 4.1.34 due to an issue in IdleStateHandler [#380](https://github.com/logstash-plugins/logstash-input-beats/pull/380)

## 6.0.5
 - Security: update netty deps - avoid CBC ciphers [#376](https://github.com/logstash-plugins/logstash-input-beats/pull/376)

## 6.0.4
 - Updated Jackson dependencies

## 6.0.3
 - Fixed configuration example in doc [#371](https://github.com/logstash-plugins/logstash-input-beats/pull/371)

## 6.0.2
 - Improved handling of invalid compressed content [#368](https://github.com/logstash-plugins/logstash-input-beats/pull/368)

## 6.0.1
 - Updated Jackson dependencies [#366](https://github.com/logstash-plugins/logstash-input-beats/pull/366)

## 6.0.0
 - Removed obsolete setting congestion_threshold and target_field_for_codec
 - Changed default value of `add_hostname` to false

## 5.1.8
 - Loosen jar-dependencies manager gem dependency to allow plugin to work with JRubies that include a later version.

## 5.1.7
 - Updated jar dependencies to reflect newer releases

## 5.1.6
 - Docs: Fixed broken link by removing extra space. [#347](https://github.com/logstash-plugins/logstash-input-beats/pull/347)

## 5.1.5
 - Docs: Fixed section ID that was causing doc build errors in the versioned
plugin docs. [#346](https://github.com/logstash-plugins/logstash-input-beats/pull/346)
 
## 5.1.4
 - Added `add_hostname` flag to enable/disable the population of the `host` field from the beats.hostname field [#340](https://github.com/logstash-plugins/logstash-input-beats/pull/340)

## 5.1.3
 - Fixed handling of batches where the sequence numbers do not start with 1 [#342](https://github.com/logstash-plugins/logstash-input-beats/pull/342)

## 5.1.2
 - Changed project to use gradle version 4.8.1. [#334](https://github.com/logstash-plugins/logstash-input-beats/pull/334)
   - This is an internal, non user-impacting, change to use a more modern version of gradle for building the plugin.

## 5.1.1
- Docs: Add more detail about creating versioned indexes for Beats data

## 5.1.0 
 - Added ssl_peer_metadata option. [#327](https://github.com/logstash-plugins/logstash-input-beats/pull/327) 
 - Fixed ssl_verify_mode => peer. [#326](https://github.com/logstash-plugins/logstash-input-beats/pull/326)

## 5.0.16
 - [#289](https://github.com/logstash-plugins/logstash-input-beats/pull/289#issuecomment-394072063) Re-initialise Netty worker group on plugin restart

## 5.0.15
  - [Ensure that context is available before trace is made](https://github.com/logstash-plugins/logstash-input-beats/pull/319/files)

## 5.0.14
  - Update jackson deps to 2.9.5

## 5.0.13
  - Fix broken 5.0.12 release

## 5.0.12
  - Docs: Set the default_codec doc attribute.

## 5.0.11
  - Ensure that the keep-alive is sent for ALL pending batches when the pipeline is blocked, not only the batches attempting to write to the queue. #310  
  
## 5.0.10
  - Update jackson deps to 2.9.4
  
## 5.0.9
  - Improvements to back pressure handling and memory management #299

## 5.0.8
  - Update jackson deps to 2.9.1

## 5.0.7
  - Docs: Deprecate `document_type` option

## 5.0.6
  - Re-order Netty pipeline to avoid NullPointerExceptions in KeepAliveHandler when Logstash is under load
  - Improve exception logging
  - Upgrade to Netty 4.1.18 with tcnative 2.0.7

## 5.0.5
  - Better handle case when remoteAddress is nil to reduce amount of warning messages in logs #269
  
## 5.0.4
  - Fix an issue with `close_wait` connection and making sure the keep alive are send back to the client all the time. #272

## 5.0.3
  - Update gemspec summary

## 5.0.2
  - Change IdleState strategy from `READER_IDLE` to `ALL_IDLE` #262
  - Additional context when logging from the BeatsHandler #261
  - Remove the `LoggingHandler` from the handler stack to reduce noise in the log.

## 5.0.1
  - Fix some documentation issues

## 5.0.0
 - Mark deprecated congestion_threshold and target_field_for_codec as obsolete

## 4.0.5
 - Additional default cipher PR#242
 - Fix logging from Java

## 4.0.4
 - Documentation fixes

## 4.0.3
 - Include remote ip_address in metadata. #180
 - Require Java 8 #221
 - Fix ability to set SSL protocols  #228

## 4.0.2
  - Relax version of concurrent-ruby to `~> 1.0` #216

## 4.0.1
 - Breaking change: Logstash will no longer start when multiline codec is used with the Beats input plugin #201

## 4.0.0
 - Version yanked from RubyGems for packaging issues

## 3.1.19 
   - Fix ability to set SSL protocols  #228
   
## 3.1.18
  - Relax version of concurrent-ruby to ~> 1.0 #216
  
## 3.1.17 
 - Docs: Add note indicating that the multiline codec should not be used with the Beats input plugin
 - Deprecate warning for multiline codec with the Beats input plugin

## 3.1.16
 - Version yanked from RubyGems for packaging issues

## 3.1.15
 - DEBUG: Add information about the remote when an exception is catched #192

## 3.1.14
 - Fix: Make sure idle connection are correctly close for the right client, #185, #178
 - Fix: remoge string interpolation for logging in critical path #184

## 3.1.13
 - Fix: remove monkeypatch from the main class to fix the documentation generator issues

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
