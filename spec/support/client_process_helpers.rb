# encoding: utf-8
require "childprocess"
module ClientProcessHelpers
  def start_client(timeout = 1)
    @client_out = Stud::Temporary.file
    @client_out.sync

    @process = ChildProcess.build(*cmd)
    @process.duplex = true
    @process.io.stdout = @process.io.stderr = @client_out
    ChildProcess.posix_spawn = true
    @process.start

    sleep(0.1)
    @client_out.rewind

    # can be used to helper debugging when a test fails
    @execution_output = @client_out.read
  end

  def stop_client
    begin
      @process.poll_for_exit(5)
    rescue ChildProcess::TimeoutError
      Process.kill("KILL", @process.pid)
    end
  end
end
