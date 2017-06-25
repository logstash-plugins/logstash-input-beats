# encoding: utf-8
require "childprocess"
module ClientProcessHelpers
  def start_client(timeout = 5)
    @client_out = Stud::Temporary.file
    @client_out.sync

    @process = ChildProcess.build(*cmd)
    @process.duplex = true
    @process.io.stdout = @process.io.stderr = @client_out
    ChildProcess.posix_spawn = true
    @process.start

    sleep_interval = 0.1
    max_iterations = (timeout / sleep_interval).to_i
    max_iterations.times do
      sleep(sleep_interval)
      if @process.alive?
        break
      end
    end
    #Note - can not raise error here if process failed to start, since some tests expects for the process to not start due to invalid configuration

    @client_out.rewind

    # can be used to helper debugging when a test fails
    @execution_output = @client_out.read
  end

  def is_alive
    return @process.alive?
  end

  def stop_client
    unless @process.nil?
      begin
        @process.poll_for_exit(5)
      rescue ChildProcess::TimeoutError
        Process.kill("KILL", @process.pid)
      end
    end
  end
end
