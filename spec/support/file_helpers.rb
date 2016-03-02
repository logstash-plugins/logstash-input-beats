# encoding: utf-8

# Add an helper method named `let_tmp_file` to spec example, this
# helper will create a temporary file with the content from the block,
# it behave like a normal `let` statement. Also to make things easier temporary
# debug it will create another `let` statement with the actual content of the block.
#
# Example:
# ```
# let_tmp_file(:hello_world_file) { "Hello world" } # return a path to a tmp file containing "Hello World"
# and will create this debug `let`, the value of the file will be the same.
# let(:hello_world_file_content) # return "Hello world"
#
module FileHelpers
  AUTO_CLEAN = true

  def self.included(base)
    base.extend(ClassMethods)
  end

  def write_to_tmp_file(content)
    file = Stud::Temporary.file
    file.write(content.to_s)
    file.close
    file.path
  end

  module ClassMethods
    def let_empty_tmp_file(name, &block)
      let(name) do
        path = nil
        f = Stud::Temporary.file
        f.close
        path = f.path
        @__let_tmp_files = [] unless @__let_tmp_files
        @__let_tmp_files << path
        path
      end
    end

    def let_tmp_file(name, &block)
      after :each do
        if @__let_tmp_files && FileHelpers::AUTO_CLEAN
          @__let_tmp_files.each do |f|
            FileUtils.rm_f(f)
          end
        end
      end

      name_content = "#{name}_content"
      let(name_content, &block)
      let(name) do
        content = __send__(name_content)
        path = write_to_tmp_file(content)
        @__let_tmp_files = [] unless @__let_tmp_files
        @__let_tmp_files << path
        path
      end
    end
  end
end
