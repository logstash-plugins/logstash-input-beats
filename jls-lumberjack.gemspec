Gem::Specification.new do |gem|
  gem.authors       = ["Jordan Sissel"]
  gem.email         = ["jls@semicomplete.com"]
  gem.description   = "lumberjack log transport library"
  gem.summary       = gem.description
  gem.homepage      = "https://github.com/jordansissel/lumberjack"

  gem.files = Dir.glob("lib/**/*.rb")

  gem.test_files    = Dir.glob("spec/**/*.rb")
  gem.name          = "jls-lumberjack"
  gem.require_paths = ["lib"]
  gem.version       = "0.0.24"

  gem.add_development_dependency "flores", "~>0.0.6"
  gem.add_development_dependency "rspec"
  gem.add_development_dependency "stud"
  gem.add_development_dependency "pry"
end
