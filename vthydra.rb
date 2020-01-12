#!/usr/bin/ruby

require 'socket'

SOCK = "/tmp/vthydrasock"
LOG = "/tmp/vthydra.log"

IOTYPES = %w(in out err)
IOS = [STDIN,STDOUT,STDERR]

class VTHydraException < Exception
end

def handle_exception e
  raise if e.is_a? SystemExit
  if e.kind_of? VTHydraException or e.kind_of? Interrupt or e.kind_of? SignalException then
    STDERR.puts e.message
  else
    STDERR.puts e.full_message.gsub(/`/,"'")
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
  def initialize io, desc, close_done=false
    @io = io
    @desc = desc
    @close_done = close_done
  end

  attr_accessor :desc, :close_done, :io

  def stream_to other
    other.stream_from self
  end

  def fileno
    @io.fileno
  end

  def cleanup
    return unless @close_done
    STDERR.puts "Cleaning up io #@desc (#{fileno})"
    @io.close
  end

  def stream_from other
    begin
      STDERR.puts "Starting iothread #{other.desc} (#{other.fileno}) -> #@desc (#{fileno})"
      buffer = []
      readerr = nil
      while true do
        begin
          r, w, e = IO.select [other.io], buffer.length ? [@io] : [] , [other.io,@io]
        rescue Errno::EBADF
          fd = @io.fileno rescue nil
          raise HydraPipeError.new "#{desc}: #{$!.message}" if ! fd
          readerr = $!
        end
        begin
          if r[0] then
            data = r[0].read_nonblock BLOCKSIZE
            if data then
              STDERR.puts "Read from #{other.desc} (#{other.fileno}) '#{data}'"
              buffer.push data
            end
          end
        rescue Errno::ECONNRESET, EOFError, IOError, Errno::EPIPE
          readerr = $!
        end
        begin
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
        rescue Errno::ECONNRESET, EOFError, IOError, Errno::EPIPE
          raise HydraPipeError.new "#{desc}: #{$!.message}"
        end
        raise HydraPipeError.new "#{other.desc}: #{readerr.message}" if readerr
      end
    ensure
      STDERR.puts "Closing iothread #{other.desc} (#{other.fileno}) -> #@desc (#{fileno})"
      other.cleanup
      cleanup
    end
  end
end

class VTHServer

  def readargs io
    args = io.gets("\0\0").split(/\0/)
  end

  def start_server machine, lpar
    cmd = %w(mkvterm -m)
    cmd.push machine
    cmd.push "-p"
    cmd.push lpar
    cmd = get_cmd *cmd
    spawn_cmd *cmd
  end

  def spawn_cmd *cmd
    STDERR.puts "Starting server thread #{cmd.inspect}"
    stdin, inpipe = IO.pipe
    outpipe, stdout= IO.pipe
    errpipe, stderr= IO.pipe
    STDERR.puts "IN: #{inpipe.fileno}:#{stdin.fileno} OUT: #{outpipe.fileno}:#{stdout.fileno} ERR: #{errpipe.fileno}:#{stderr.fileno}"
    [ inpipe, stdin, stdout, outpipe, stderr, errpipe].each{|fd| fd.sync = true}
    pid = spawn(*cmd, :err=>stderr, :out=>stdout, :in=>stdin, :close_others=>true)
    STDERR.puts "Started server thread #{cmd.inspect} #{pid}"
    addfds inpipe, outpipe, errpipe
    [stdin, stdout, stderr].each{|fd| fd.close}
    server = [pid, inpipe.fileno, outpipe.fileno, errpipe.fileno]
  end

  def get_server machine, lpar
    key = "#{machine}\0#{lpar}"
    server = @servers[key]
    begin
      server[1..-1].each{|fd| IO.for_fd fd} if server
    rescue
      remove_server server
      server = nil
    end
    return server if server
    return @servers[key] = (start_server machine, lpar)
  end

  def addfds *fds
    fds.each{|fd| @fd[fd.fileno] = fd}
  end

  def removefds *fds
    fds.each{|fd| @fd.delete fd}
  end

  def remove_server server
    removefds *server[1..-1]
    @servers.delete_if{|k,s| s == server} if server
  end

  def client_sock type, io, main_io, fd
    begin
      STDERR.puts "#{io.fileno}: #{IOTYPES[type]}sock IO thread starting (#{main_io}, #{fd})"
      main_io = main_io.to_i
      fd = fd.to_i
      if fd != 0 then
        pipe = IO.for_fd fd rescue nil
        pipedesc = "pipe:#{fd}"
        if pipe then
          pipe = IODesc.new pipe, pipedesc
        else
          raise HydraPipeError.new "#{pipedesc}: Bad file descriptor"
        end
      end
    end
    @clients[main_io] = [] if ! @clients[main_io]
    @clients[main_io][type] = io
    io.write("\0");
    desc = IODesc.new io, "#{IOTYPES[type]}sock (#{main_io}, #{fd})"
    if type > 0 then
      desc.stream_from pipe
    else
      desc.stream_to pipe
    end
  ensure
    if fd !=0 then
      STDERR.puts "#{io.fileno}: #{IOTYPES[type]}sock IO thread stopping (#{main_io}, #{fd})"
      io.close
    end
  end

  def insock io, main_io, fd
    client_sock 0, io, main_io, fd
  end

  def outsock io, main_io, fd
    client_sock 1, io, main_io, fd
  end

  def errsock io, main_io, fd
    client_sock 2, io, main_io, fd
  end

  def end_client io
    clients.each{|c|c.each{|io|io.close rescue nil}}
  end

  def servercmd io, *args
    connectsocks io, [0]*4
    args.each{|a|
      case a
      when /^Kill!$/
        STDERR.puts "Killing server."
        @clients[io.fileno][1].puts "Killing server."
        exit 0
      else
        STDERR.puts "#{io.fileno}: Unknown command"
        @clients[io.fileno][1].puts "Unknown command."
      end
    }
    io.close
    end_client io
  end

  def do_spawn io, *args
    STDERR.puts "#{io.fileno}: Starting server for #{args.inspect}"
    server = spawn_cmd *args
    STDERR.puts "#{io.fileno}: Started server #{server.inspect}"
    connectsocks io, server
    waitserver io, server
  end

  def mkvterm io, *args
    STDERR.puts "#{io.fileno}: Getting server connection for #{args.inspect}"
    *machine = find_machine_arg *args
    STDERR.puts "#{io.fileno}: Found machine #{machine.inspect}"
    server = get_server *machine
    STDERR.puts "#{io.fileno}: Found server #{server.inspect}"
    connectsocks io, server
    waitserver io, server
  end

  def connectsocks io, server
    io.write "#{io.fileno}\0"
    io.write "#{server[1..3].join("\0")}\0"
    io.getc
  end
  def waitserver io, server
    begin
      STDERR.puts "#{io.fileno}: Waiting for #{server[0]}"
      _, status = Process.wait server[0]
      io.write "#{status}\0"
      STDERR.puts "#{io.fileno}: #{server[0]} exited, cleaning up"
    ensure
      server[1..-1].each{|fileno| IO.for_fd(fileno).close rescue nil} if server
      remove_server server
      STDERR.puts "#{io.fileno}: Terminating"
      io.close
    end
  end

  def serve io
    begin
      STDERR.puts "#{io.fileno}: New thread starting"
      io.sync = true
      args = readargs io rescue nil
      if not args then
        STDERR.puts "#{io.fileno}: did not get arguments, closing."
        io.close
        return 0
      end
      STDERR.puts "#{io.fileno}: got arguments #{args.inspect}"
      command, *args = args
      return insock io, *args if command =~ /^insock$/
      return outsock io, *args if command =~ /^outsock$/
      return errsock io, *args if command =~ /^errsock$/
      return mkvterm io, *args if command =~ /^mkvterm$/
      return do_spawn io, *args if command =~ /^spawn$/
      return servercmd io, command, *args
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
    @servers={}
    @clients={}
    @fd={}
    sock = Socket.unix_server_socket(SOCK)
    sock.listen 10
    while conn = sock.accept do
      io, address = conn
      STDERR.puts "#{io.fileno}: Accepted connection from '#{address}'"
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
    sock.write @args.join("\0")+"\0\0"
    @cli_key = readint sock
    @sock_keys = (0..2).map{readint sock}
    @sock = sock
  end

  def cli_thread type, key
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
    Thread.new {
      begin
        stdio.send method, desc
      rescue Object
        handle_exception $!
      end
    }
  end

  def passthru
    threads=@sock_keys.to_enum(:each_with_index).map{|key,i| cli_thread i, key}
    @sock.write("\0")
    threads.reverse.each{|t|t.join}
  end

  def run
    STDERR.puts "Starting '#{@args.join("' '")}'"
    begin
      connectsock
    rescue
      STDERR.puts "Connecting to server failed, trying to for one"
      if not fork then
        STDERR.puts "Forked daemon"
        VTHServer.new
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
    raise HydraConnectError.new  unless @sock
    passthru
    exit readint
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
