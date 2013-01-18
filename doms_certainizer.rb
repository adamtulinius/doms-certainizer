require 'open-uri'
require 'erb'
require 'optparse'
require 'rexml/document'

OptionParser.new do |o|
    o.on('-c', '--config CONFIG') do |config_file|
        config = REXML::Document.new (File.open(config_file))
        config.get_elements('/config/host').each {|host| $host ||= host.text}
        config.get_elements('/config/port').each {|port| $port ||= port.text.to_i}
        config.get_elements('/config/username').each {|username| $username ||= username.text}
        config.get_elements('/config/password').each {|password| $password ||= password.text}
        config.get_elements('/config/datastreams/datastream').each do |datastream|
            $required_datastreams ||= []
            $required_datastreams << datastream
        end
    end
    o.on('-h', '--host HOST') {|host| $host = host}
    o.on('-p', '--port PORT') {|port| $port = port.to_i}
    o.on('-i', '--input FILENAME') {|input| $input_file = input}
    o.on('-o', '--output FILENAME') { |output| $output_file = output }
    o.on('-e', '--extra FILENAME') { |file| $extra_files = File.open(file, "w") }
    o.on('-m', '--missing FILENAME') { |file| $missing_files = File.open(file, "w") }
    o.on('-l', '--limit LIMIT') {|limit| $limit = limit.to_i}
    o.on('-s', '--state required state') {|state| $required_state = state}
    o.on('-r', '--required FILENAME') do |file|
        $required_files ||= []
        File.open(file).each_line do |line|
            match = line.match /[0-9a-f]{32} \d+ (?<filename>.+)/
            if match
                # line containing <checksum> <size> <filename>
                $required_files << match[:filename]
            else
                # line only containing <filename>
                $required_files << line
            end
        end
        $required_files.sort!
    end
    o.on_tail('--help') { puts o; exit }
    o.parse!
end

if $host and $input_file
    puts "Only one of -u/--url and -i/--input allowed."
    exit
end

# defaults
$host ||= "localhost" unless $input_file # only set $host if $input_file hasn't already been set
$port ||= 7880
$username ||= "fedoraAdmin"
$password ||= "fedoraAdminPass"
$limit ||= 0
$required_datastreams ||= %w(BROADCAST_METADATA FFPROBE FFPROBE_ERRORS)
$required_files ||= []

##################################################################
# move into other file

class Timer
    attr_reader :runtime
    def initialize(msg=nil)
        @chatty = !msg.nil?
        @start = Time.now
        print "#{msg} .. " if @chatty
    end

    def checkpoint
        @runtime = Time.now - @start
    end

    def stop
        checkpoint
        puts "done! (#{@runtime}s)" if @chatty
        @runtime
    end
end

class OutputWriter
    attr_reader :persist, :file
    def initialize(file=nil)
        @persist = !!file
        if @persist
            @file = File.open(file, "w")
        end
    end

    def write(object)
        line = [object[:uuid],
                object[:filename],
                object[:missing_datastreams].join(" ")].join("\t") + "\n"
        if @persist
            @file.puts(line)
        else
            puts line
        end
    end
end

#
##################################################################

class DomsCertainizer
    attr_accessor :report_interval
    attr_reader :loop_time, :parse_time, :average, :processed_items, :objects_found

    def initialize
        @output = OutputWriter.new($output_file)

        @query = %q{
            select $pid $state $label $datastream
            from <#ri>
            where
            $pid <fedora-model:state> $state
            and
            $pid <fedora-model:label> $label
            and
            $pid <fedora-view:disseminates> $datastreamID
            and
            $pid <fedora-model:hasModel> <info:fedora/doms:ContentModel_RadioTVFile>
            and
            $pid <http://doms.statsbiblioteket.dk/relations/default/0/1/#isPartOfCollection> $collection
            and
            $datastreamID <fedora-view:disseminationType> $datastream
            order by $pid
        }

        # Everybody stand back!
        uuid_regex = "info\:fedora\/(?<uuid>[^\t]+)"
        status_regex = "[^#]+#(?<state>[^\t]+)"
        filename_regex = ".+\/(?<filename>[^\t\/]+)"
        datastream_regex = ".+?(?<datastream>[a-zA-Z0-9_]+)"
        @regex = /^#{uuid_regex}\t#{status_regex}\t#{filename_regex}\t#{datastream_regex}$/

        @report_interval = 5000
        @parse_time = 0
        @average = 0
        @processed_items = 0
        @objects_found = []
    end

    def escaped_query
        ERB::Util.url_encode @query
    end

    def uri
        "http://#{$host}:#{$port}/fedora/risearch?lang=itql&format=tsv&limit=#{$limit}&query=#{escaped_query}"
    end

    def run
        if $host
            t = Timer.new("Exchanging bits with #{$host}:#{$port}")
            input = open(uri, :http_basic_authentication => [$username, $password])
            t.stop
        else
            t = Timer.new("Taking a good look at '#{$input_file}'")
            input = open($input_file)
            t.stop
        end

        loop_timer = Timer.new
        current_object = nil
        input.drop(1).each do |line|
            process_timer = Timer.new

            regex_timer = Timer.new
            match = @regex.match line
            @parse_time += regex_timer.stop

            if match
                if current_object == nil or match[:filename] != current_object[:filename]
                    if current_object != nil
                        complete_object(current_object)
                    end

                    current_object = Hash[:uuid, match[:uuid],
                                          :state, match[:state],
                                          :filename, match[:filename],
                                          :missing_datastreams, $required_datastreams.clone]
                end
                current_object[:missing_datastreams].delete match[:datastream]
            else
                puts "Unexpected input: #{line}"
            end

            # calculate cumulative moving average
            @average = (@average*@processed_items + process_timer.stop)/(@processed_items+1)
            @processed_items += 1

            if @processed_items % @report_interval == 0
                puts "Processed #{@processed_items} at #{1/@average}/s"
            end
        end

        complete_object(current_object)
        @loop_time = loop_timer.stop
    end

    def print_statistics
        puts "avg: #{average}s (#{1/average}/s)"
        puts "loop time: #{loop_time}"
        puts "parse time: #{parse_time}"
    end

    private
    def complete_object(current_object)
        @objects_found << current_object
    end
end


if __FILE__ ==  $0
    dc = DomsCertainizer.new
    dc.run
    dc.print_statistics
    puts

    objects_found = dc.objects_found
    objects_found = objects_found.select {|object| object[:state] == $required_state} if $required_state
    files_found = objects_found.map {|object| object[:filename]}

    missing = $required_files - files_found
    if $missing_files
        $missing_files.puts missing
    else
        puts missing.map {|file| "-#{file}"}
    end

    extra = files_found - $required_files
    if $extra_files
        $extra_files.puts extra
    else
        puts extra.map{|file| "+#{file}"}
    end
end
