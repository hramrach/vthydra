#!/usr/bin/ruby

# vim:set shiftwidth=2 expandtab:

require 'socket'

SOCK = "/tmp/vthydrasock"
LOG = "/tmp/vthydra.log"

IOTYPES = %w(in out err)
IOS = [STDIN,STDOUT,STDERR]

class Array
  def mapm method, *args
    self.map{|elt| elt.send method, *args}
  end
end

class VTHydraException < Exception
end

def handle_exception e, description=nil
  raise if e.is_a? SystemExit
  description = (description ? description + ": " : "")
  if e.kind_of? VTHydraException or e.kind_of? Interrupt or e.kind_of? SignalException then
    STDERR.puts description + e.message
  else
    STDERR.puts description + e.full_message.gsub(/`/,"'")
  end
end

class HydraConnectError < VTHydraException
  def initialize msg="Error connecting to server."
    super msg
  end
end

class HydraPipeError < VTHydraException
  def initialize msg
    super msg
  end
end

class HydraUnknownCommandError < VTHydraException
  def initialize *args
    super "Unknown command: '#{args.join "' '" }'"
  end
end

def find_machine_arg *args
  machine = lpar = nil
  (0...args.length).each{|i|
    machine = args[i+1] if args[i] == '-m'
    lpar = args[i+1] if args[i] == '-p'
  }
  [machine,lpar]
end

def get_hmc system, lpar
  "hscroot@powerhmc2.arch.suse.de"
end

def get_ssh_args command
  args = %w(-q -e none)
  case command
  when /^mkvterm|rmvterm$/
    args.push "-t"
  end
  args
end

def get_cmd command, *args
  ["ssh", *(get_ssh_args command), (get_hmc *(find_machine_arg *args)), command, *args]
end

class IODesc
  BLOCKSIZE = 4096
  def initialize io, desc, close_finnished=false
    @io = io
    @fileno = io.fileno
    @desc = desc
    @close_finnished = close_finnished
  end

  attr_accessor :close_finnished
  attr_accessor :desc, :io, :fileno

  def stream_to other
    other.stream_from self
  end

  def cleanup
    return unless @close_finnished
    STDERR.puts "Cleaning up io #@desc (#{fileno})"
    @io.close
  end

  def stream_from other
    begin
      stream_desc = "#{other.desc} (#{other.fileno}) -> #@desc (#{fileno})"
      STDERR.puts "Starting iothread " + stream_desc
      buffer = []
      readerr = nil
      while true do
        begin
          r, w, _, e = IO.select_with_poll [other.io], (buffer[0] ? [@io] : []) , [], [other.io,@io]
          STDERR.puts "Select #{stream_desc} b:#{buffer.inspect} r:#{r.inspect} w:#{w.inspect} e:#{e.inspect}"
        rescue Errno::EBADF
          fd = @io.fileno rescue nil
          raise HydraPipeError.new "#{desc}: #{$!.message}" if ! fd
          readerr = $!
        end
        e.each{|io|
          if @io == io then
            raise HydraPipeError.new "#{desc}: Poll error on descriptor"
          else
            raise HydraPipeError.new "#{other.desc}: Poll error on descriptor"
          end
        }
        begin
          if r && r[0] then
            data = r[0].read_nonblock BLOCKSIZE
            if data then
              STDERR.puts "Read from #{other.desc} (#{other.fileno}) '#{data}'"
              buffer.push data
            end
          end
        rescue Errno::ECONNRESET, EOFError, IOError, Errno::EPIPE, Errno::EBADF
          readerr = $!
        end
        begin
          @io.write ""
          while buffer[0] do
            data = buffer.shift
            written = @io.write_nonblock data
            STDERR.puts "Written to #{@desc} (#{fileno}) '#{data[0...written]}'"
            if written < data.length then
              buffer.unshift data[written..-1]
              break
            end
            break if ! buffer[0]
          end
        rescue Errno::ECONNRESET, EOFError, IOError, Errno::EPIPE, Errno::EBADF
          raise HydraPipeError.new "#{desc}: #{$!.message}"
        end
        raise HydraPipeError.new "#{other.desc}: #{readerr.message}" if readerr
      end
    ensure
      STDERR.puts "Closing iothread " + stream_desc
      other.cleanup
      cleanup
    end
  end
end

class VTHServer
  attr_accessor :persistent
  attr_reader :pid, :cmd
  def initialize *cmd
    @cmd = cmd
    @clients = []
    STDERR.puts "Starting server thread #{cmd.inspect}"
    @stdin, @inpipe = IO.pipe
    @outpipe, @stdout= IO.pipe
    @errpipe, @stderr= IO.pipe
    STDERR.puts "IN: #{@inpipe.fileno}:#{@stdin.fileno} OUT: #{@outpipe.fileno}:#{@stdout.fileno} ERR: #{@errpipe.fileno}:#{@stderr.fileno}"
    [ @inpipe, @stdin, @stdout, @outpipe, @stderr, @errpipe].each{|fd| fd.sync = true}
    @persistent = false
    @status = false
    @status_mutex = Mutex.new
  end
  def run
    return if !@cmd || @cmd.length == 0
    STDERR.puts "Starting server thread #{@cmd.inspect}"
    begin
      @pid = spawn(*@cmd, :err=>@stderr, :out=>@stdout, :in=>@stdin, :close_others=>true)
      [@stdin, @stdout, @stderr].each{|fd| fd.close}
      Thread.new{
        begin
          wait_cmd
        rescue Object
          handle_exception $!
        end
      }
      STDERR.puts "Started server thread #{@cmd.inspect} #{@pid.inspect}"
    rescue Object
      begin
        @stderr.puts $!.message.gsub(/`/,"'")
      rescue Object
        handle_exception $!, "Reporting spawn error to client"
      end
      @cmd = nil
      handle_exception $!
      cleanup
    end
  end
  def fds
    [@inpipe, @outpipe, @errpipe].mapm :to_i
  end
  def puts *args
    @stdout.puts *args
  end
  def running
    !@finished
  end
  def add_client cli
    @clients << cli
  end
  def remove_client cli
    @clients.delete cli
    cleanup unless @clients.length
  end
  def cleanup
    return if @persistent && !@finished
    [ @stdin, @stdout, @stderr].each{|fd| fd.close rescue nil} unless @cmd && @cmd.length
  end
  def wait_cmd
    @status_mutex.synchronize {
      return @status if @status
      Process.waitpid @pid, 0 rescue nil
      @status = $?
      @finished = true
      @status
    }
  end
end

class VTHClientConnection
  @@servers = {}
  @@clients = {}

  def readargs
    begin
      args = @io.gets("\0\0").split(/\0/)
    rescue Object
      handle_exception $!
      nil
    end
  end

  def initialize io
    @io = io
    @fileno = io.fileno
    args = readargs
    if not args then
      STDERR.puts "#{@fileno}: did not get arguments, closing."
      @io.close
    end
    STDERR.puts "#{@fileno}: got arguments #{args.inspect}"
    @command, *@args = args
    if @command =~ /^(in|out|err)sock$/ then
      @command.sub!(/sock$/,'')
      return sock
    end
    @@clients[@fileno] = []
    return mkvterm if @command =~ /^mkvterm$/
    return do_spawn if @command =~ /^spawn$/
    @args = args
    return servercmd
  end

  def sock
    begin
      @type =  IOTYPES.find_index(@command)
      @main_io, @fd = @args.mapm :to_i
      STDERR.puts "#{@fileno}: #{IOTYPES[@type]}sock IO thread starting (#{@main_io}, #{@fd})"
      if @fd != 0 then
        @pipe = IO.for_fd @fd rescue nil
        pipedesc = "pipe:#{@fd}"
        if @pipe then
          @pipe = IODesc.new @pipe, pipedesc, true
        else
          raise HydraPipeError.new "#{pipedesc}: Bad file descriptor"
        end
      end
    end
    @@clients[@main_io][@type] = @io
    @io.write("\0");
    @desc = IODesc.new @io, "#{IOTYPES[@type]}sock (#{@main_io}, #{@fd})", true
    if @type > 0 then
      @desc.stream_from @pipe
    else
      @desc.stream_to @pipe
    end
  ensure
    if @fd !=0 then
      STDERR.puts "#{@fileno}: #{IOTYPES[@type]}sock IO thread stopping (#{@main_io}, #{@fd})"
      @io.close rescue nil
    end
  end

  def start_server
    cmd = %w(mkvterm -m)
    cmd.push @machine
    cmd.push "-p"
    cmd.push @lpar
    cmd = get_cmd *cmd
    @server = VTHServer.new *cmd
    @server.persistent = true
    @server
  end

  def get_server
    @key = "#{@machine}\0#{@lpar}"
    @server = @@servers[@key]
    if ! @server.pid then
      remove_server @server
      @server = nil
    end
    return @server if @server
    return @@servers[@key] = start_server
  end

  def remove_server server
    @@servers.delete_if{|k,s| s == server} if server
  end

  def end_client
    @@clients[@fileno].each{|s| s.close rescue nil }
    @io.close
  end

  def servercmd
    @server = VTHServer.new
    connectsocks
    @args.each{|a|
      case a
      when /^Kill!$/
        STDERR.puts "Killing server."
        @server.puts "Killing server."
        @io.write "0\0"
        exit 0
      else
        STDERR.puts "#{@fileno}: Unknown command"
        @server.puts "Unknown command."
        @io.write "255\0"
      end
    }
    @server.cleanup
    end_client
  end

  def do_spawn
    STDERR.puts "#{@fileno}: Starting server for #{@args.inspect}"
    @server = VTHServer.new *@args
    STDERR.puts "#{@fileno}: Set up server #{@server.inspect}"
    connectsocks
    waitserver
  end

  def mkvterm
    STDERR.puts "#{@fileno}: Getting server connection for #{@args.inspect}"
    @machine, @lpar = find_machine_arg *@args
    STDERR.puts "#{@fileno}: Found machine #{machine.inspect}"
    @server = get_server
    STDERR.puts "#{@fileno}: Found server #{@server.inspect}"
    connectsocks
    waitserver
  end

  def connectsocks
    @io.write "#{@fileno}\0"
    @io.write "#{@server.fds.join("\0")}\0"
    @io.getc
    @io.getc
    @io.getc
    @io.getc
  end

  def waitserver
    begin
      STDERR.puts "#{@fileno}: Starting server #{@server.cmd.inspect}"
      @server.run
      STDERR.puts "#{@fileno}: Waiting for #{@server.pid}"
      status = @server.wait_cmd
      @io.write "#{status}\0"
    ensure
      STDERR.puts "#{@fileno}: #{@server.pid} exited, cleaning up"
      @server.cleanup
      remove_server @server unless @server.running
      STDERR.puts "#{@fileno}: Terminating"
      @io.close
    end
  end

end

class VTHServerDispatcher

  def serve io
    begin
      STDERR.puts "#{io.fileno}: New thread starting"
      io.sync = true
      VTHClientConnection.new io
    rescue Object
      handle_exception $!
    end
  end

  def initialize
    STDIN.close
    Process.setsid
    $stderr.reopen(LOG,"at")
    $stderr.sync = true
    $stdout.reopen(LOG,"at")
    $stdout.sync = true
    STDERR.puts "Starting daemon"
    sock = Socket.unix_server_socket(SOCK)
    sock.listen 10
    while conn = sock.accept do
      io, address = conn
      STDERR.puts "#{io.fileno}: Accepted connection from '#{address.inspect}'"
      Thread.new(io){|io| serve io }
    end
    exit 0
  end
end

class VTHClient
  def initialize *args
    @args = args
    @sock = nil
  end

  def readint sock=@sock
    sock.gets("\0").chomp("\0").to_i
  end

  def newsock
    sock = Socket.unix(SOCK)
    sock.sync = true
    sock
  end

  def connectsock
    sock = newsock
    @connected = true
    sock.write @args.join("\0")+"\0\0"
    @cli_key = readint sock
    @sock_keys = (0..2).map{readint sock}
    @sock = sock
  end

  def io_thread type, key
    sock = newsock
    sock.write "#{IOTYPES[type]}sock\0#@cli_key\0#{key}\0\0"
    sock.getc
    stdio = IODesc.new IOS[type], "std#{IOTYPES[type]}"
    desc = IODesc.new sock, "#{IOTYPES[type]}sock #@cli_key #{key}", true
    if type > 0 then
      sock.close_write
      method = :stream_from
    else
      sock.close_read
      method = :stream_to
    end
    t = Thread.new {
      begin
        stdio.send method, desc
      rescue Object
        handle_exception $!
      end
    }
    @sock.write("\0")
    t
  end

  def passthru
    threads=@sock_keys.to_enum(:each_with_index).map{|key,i| io_thread i, key}
    @sock.write("\0")
    threads.reverse.each{|t|t.join}
  end

  def run
    STDERR.puts "Starting '#{@args.join("' '")}'"
    begin
      connectsock
    rescue
      STDERR.puts "Connecting to server failed, trying to fork one"
      if ! @connected then
        if not fork then
          STDERR.puts "Forked daemon"
          VTHServerDispatcher.new
        else
          STDERR << "Waiting for sockserver to start"
          (1..3).each{
            connectsock rescue nil
            break if @sock
            sleep 1
            STDERR << "."
          }
          STDERR << "\n"
        end
      end
    end
    raise HydraConnectError.new  unless @sock
    passthru
    begin
      exit readint
    rescue Object
      handle_exception $!
      exit -Errno::EPIPE::Errno
    end
  end
end

class VTHydra

  def do_dspmsg
    args = @args
    while true do
      case args[0]
      when /^-s$/
        args=args[2..-1]
      when %r|^/usr/share/locale/en/LC_MESSAGES/IBMhsc.netboot.cat$|
        args=args[2..-1]
      else
        break;
      end
    end
    exec "/usr/bin/printf", *args
  end

  def do_copystream
    $stderr.reopen(LOG,"at")
    $stderr.sync = true
    stdin = IODesc.new STDIN, "stdin"
    stdout = IODesc.new STDOUT, "stdout"
    stdin.stream_to stdout
  end

  def do_cmd
    exec *get_cmd
  end

  def do_client
    STDERR.puts "do_client '#@command'  '#{@args.join("' '")}'"
    cli = VTHClient.new @command, *@args
    cli.run
  end

  def msg
    "! '#{[@command, *@args].join("' '")}'"
  end

  def log logfile
    File.open(logfile,"at"){|log| log.puts msg }
  end

  def run
    case @command
    when /^dspmsg$/
      do_dspmsg
    when /^copystream$/
      do_copystream
    when /^chsysstate|lscomgmt|lssyscfg$/
      do_cmd
    when /^rmvterm$/
      exit 0
    else
      do_client
    end
  end

  def initialize command, *args
    STDERR.puts "Init '#{command}'  '#{args.join("' '")}'"
    if command =~ /^vthydra|vthydra[.]rb$/ ; then
      @command, *@args = args
    else
      @command = command
      @args = args
    end
    STDERR.puts "Init '#@command'  '#{@args.join("' '")}'"
  end

end

begin
  task = VTHydra.new File.basename($0), *ARGV
  task.log LOG
  task.run
rescue Object
  handle_exception $!
  exit 255
end
