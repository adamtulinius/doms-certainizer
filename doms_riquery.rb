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
    end
    o.on('-h', '--host HOST') {|host| $host = host}
    o.on('-p', '--port PORT') {|port| $port = port.to_i}
    o.on('-u', '--username USERNAME') {|username| $username = username}
    o.on('--password PASSWORD') {|password| $password = password}
    o.on('-l', '--limit LIMIT') {|limit| $limit = limit.to_i}
    o.on_tail('--help') { puts o; exit }
    o.parse!
end


# defaults
$host ||= "localhost"
$port ||= 7880
$username ||= "fedoraAdmin"
$password ||= "fedoraAdminPass"
$limit ||= 0

class DomsRiquery
    def initialize(query)
        @query = query
    end

    def escaped_query
        ERB::Util.url_encode @query
    end

    def uri
        "http://#{$host}:#{$port}/fedora/risearch?lang=itql&format=tsv&limit=#{$limit}&query=#{escaped_query}"
    end

    def run
        t_start = Time.now
        input = open(uri, :http_basic_authentication => [$username, $password])
        puts "#{input.count} results in #{Time.now - t_start}s"
    end
end


if __FILE__ ==  $0
    query = %q{
            select $object $date
            from <#ri>
            where
            $object <info:fedora/fedora-system:def/model#hasModel> $cm
            and
            $cm <http://ecm.sourceforge.net/relations/0/2/#isEntryForViewAngle> 'SummaVisible'
            and
            $object <http://doms.statsbiblioteket.dk/relations/default/0/1/#isPartOfCollection> <info:fedora/doms:RadioTV_Collection>
            and
            $object <info:fedora/fedora-system:def/model#state> <info:fedora/fedora-system:def/model#Active>
            and
            $object <info:fedora/fedora-system:def/view#lastModifiedDate> $date
            order by $date asc
            limit 10000
        }
    dq = DomsRiquery.new(query)
    dq.run
end
