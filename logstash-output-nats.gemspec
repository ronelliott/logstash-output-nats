Gem::Specification.new do |s|
  s.name = "logstash-output-nats"
  s.version = "1.0.0"
  s.licenses = ["Apache-2.0"]
  s.summary = "This output writes to Nats."
  s.description = "This gem is a Logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/logstash-plugin install gemname. This gem is not a stand-alone program"
  s.authors = ["Nick Carenza"]
  s.email = "info@elastic.co"
  s.homepage = "http://www.elastic.co/guide/en/logstash/current/index.html"
  s.require_paths = ["lib"]

  # Files
  s.files = Dir["lib/**/*","spec/**/*","vendor/**/*","*.gemspec","*.md","Gemfile","LICENSE","NOTICE.TXT"]
   # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "output" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", "~> 1.0"
  s.add_runtime_dependency "nats", "~> 0.7"
  # s.add_runtime_dependency "logstash-core", ">= 2.0.0", "< 3.0.0"

  s.add_development_dependency "logstash-codec-json"
  s.add_development_dependency "logstash-devutils"
end