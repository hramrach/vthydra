#!/usr/bin/ruby

$command=File.basename($0)

def get_hmc system, lpar
"hscroot@powerhmc2.arch.suse.de"
end

def get_ssh_args
case $command
when /^mkvterm|rmvterm$/
%w(-q -t -e none)
else
%w(-q -e none)
end
end

def find_machine_arg *args
machine = lpar = nil
(0...args.length).each{|i|
marchine = args[i+1] if args[i] == '-m'
lpar = args[i+1] if args[i] == '-p'
}
[machine,lpar]
end

def do_cmd
exec "ssh", *get_ssh_args, (get_hmc *(find_machine_arg *ARGV)), $command, *ARGV
end

def do_dspmsg
args=ARGV
while 1 do
case args[0]
when /^-s$/
args=args[2..]
when %r|^/usr/share/locale/en/LC_MESSAGES/IBMhsc.netboot.cat$|
args=args[2..]
else
break;
end
end
exec "/usr/bin/printf", *args
end

begin
msg = "! #$command #{ARGV.join(" ")}"
File.open("/tmp/vthydra.log","at"){|log| log.puts msg }
case $command
when /^dspmsg$/
do_dspmsg
when /^chsysstate|lscomgmt|lssyscfg|mkvterm|rmvterm$/
do_cmd
when /^rmvterm$/
exit 0
else
STDERR.puts msg
end
rescue
STDERR.puts $!.full_message.gsub(/`/,"'")
exit 255
end
