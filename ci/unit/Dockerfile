ARG ELASTIC_STACK_VERSION
FROM docker.elastic.co/logstash/logstash:$ELASTIC_STACK_VERSION
COPY --chown=logstash:logstash Gemfile /usr/share/plugins/plugin/Gemfile
COPY --chown=logstash:logstash *.gemspec /usr/share/plugins/plugin/
COPY --chown=logstash:logstash VERSION /usr/share/plugins/plugin/
RUN cp /usr/share/logstash/logstash-core/versions-gem-copy.yml /usr/share/logstash/versions.yml
ENV PATH="${PATH}:/usr/share/logstash/vendor/jruby/bin"
ENV LOGSTASH_SOURCE=1
ENV JARS_SKIP="true"
RUN gem install bundler -v "~> 1"
WORKDIR /usr/share/plugins/plugin
RUN bundle install
COPY --chown=logstash:logstash src /usr/share/plugins/plugin/src
COPY --chown=logstash:logstash Rakefile /usr/share/plugins/plugin/
COPY --chown=logstash:logstash gradle /usr/share/plugins/plugin/gradle
COPY --chown=logstash:logstash gradlew /usr/share/plugins/plugin/
COPY --chown=logstash:logstash build.gradle /usr/share/plugins/plugin/
COPY --chown=logstash:logstash settings.gradle /usr/share/plugins/plugin/
COPY --chown=logstash:logstash . /usr/share/plugins/plugin
RUN bundle exec rake vendor
