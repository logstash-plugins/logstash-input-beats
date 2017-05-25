if [ "${LOGSTASH_BRANCH}" = "feature/9000" ]; then
    exit 1
fi
bundle install
bundle exec rake vendor
./gradlew test
bundle exec rspec spec
bundle exec rake test:integration:setup
bundle exec rspec spec --tag integration
