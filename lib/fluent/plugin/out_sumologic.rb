# encoding: UTF-8
require 'net/http'
require 'date'

class Fluent::SumologicOutput< Fluent::BufferedOutput
  Fluent::Plugin.register_output('sumologic', self)

  config_param :host, :string,  :default => 'collectors.sumologic.com'
  config_param :port, :integer, :default => 443
  config_param :path, :string,  :default => '/receiver/v1/http/XXX'
  config_param :format, :string, :default => 'json'
  config_param :source_name, :string, :default => ''

  include Fluent::SetTagKeyMixin
  config_set_default :include_tag_key, false

  include Fluent::SetTimeKeyMixin
  config_set_default :include_time_key, false

  def initialize
    super
  end

  def configure(conf)
    super
  end

  def start
    super
  end

  def format(tag, time, record)
    [tag, time, record].to_msgpack
  end

  def shutdown
    super
  end

  def client
    return @_client if @_client

    @_client = Net::HTTP.new(@host, @port.to_i)
    @_client.use_ssl = true
    @_client.verify_mode = OpenSSL::SSL::VERIFY_PEER
    @_client
  end

  def write(chunk)
    messages = []
    
    case @format
      when 'json'
        chunk.msgpack_each do |tag, time, record|
          if @include_tag_key
            record.merge!(@tag_key => tag)
          end
          if @include_time_key
            record.merge!(@time_key => @timef.format(time))
          end
          messages << record.to_json
        end
      when 'text'
        chunk.msgpack_each do |tag, time, record|
          messages << record['message']
        end
    end

    header = {}
    header['X-Sumo-Name'] = @source_name unless @source_name.empty?
    begin
      client.post(@path, messages.join("\n"), header)
    rescue Errno::ECONNREFUSED, Errno::ETIMEDOUT => e
      @_client = nil
      raise ConnectionFailure, "Could not push logs to SumoLogic. #{e.message}"
    end
  end
end
