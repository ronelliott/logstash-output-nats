# encoding: utf-8

require "logstash/outputs/base"
require "logstash/namespace"

require "json"
require "thread"

require "nats/client"

# require_relative "nats/nats_connection"

# A NATS output for logstash
class LogStash::Outputs::Nats < LogStash::Outputs::Base
  conn = nil

  default :codec, "json"

  config_name "nats"

  # The subject to use
  config :subject, :validate => :string, :required => true

  # The hostname or IP address to reach your NATS instance
  config :server, :validate => :string, :default => "nats://0.0.0.0:4222", :required => true

  # This sets the concurrency behavior of this plugin. By default it is :legacy, which was the standard
  # way concurrency worked before Logstash 2.4
  #
  # You should explicitly set it to either :single or :shared as :legacy will be removed in Logstash 6.0
  #
  # When configured as :single a single instance of the Output will be shared among the
  # pipeline worker threads. Access to the `#multi_receive/#multi_receive_encoded/#receive` method will be synchronized
  # i.e. only one thread will be active at a time making threadsafety much simpler.
  #
  # You can set this to :shared if your output is threadsafe. This will maximize
  # concurrency but you will need to make appropriate uses of mutexes in `#multi_receive/#receive`.
  #
  # Only the `#multi_receive/#multi_receive_encoded` methods need to actually be threadsafe, the other methods
  # will only be executed in a single thread
  # concurrency :shared
  # ^ seems to frag plugin loading, "NoMethodError"
  concurrency :single

  def register
    @codec.on_event &method(:send_to_nats)
  end

  def get_nats_connection
    if @conn == nil
      @conn = NATSConnection.new @server, @logger
    end

    @conn
  end

  # Needed for logstash < 2.2 compatibility
  # Takes events one at a time
  def receive(event)
    if event == LogStash::SHUTDOWN
      return
    end

    begin
      @logger.debug "NATS: Encoding event"
      @codec.encode event
    rescue Exception => e
      @logger.warn "NATS: Error encoding event", :exception => e, :event => event
    end
  end

  def send_to_nats(event, payload)
    key = event.sprintf @subject
    @logger.debug "NATS: Publishing event to #{key}"

    conn = nil

    begin
      conn = get_nats_connection
      conn.publish key, payload
    rescue => e
      @logger.warn("NATS: failed to send event",
        :event => event,
        :exception => e,
        :backtrace => e.backtrace)
      retry
    end
  end
end

class NATSConnection
  def initialize(uri, logger)
    @logger = logger
    @queue = Queue.new
    @uris = [ uri ]

    Thread.new do
      connection_handler
    end

    super()
  end

  public
  def block_until_empty
    # block until the queue is empty
    until !has_queued_messages? do
    end
  end

  public
  def has_queued_messages?
    !@queue.empty?
  end

  public
  def queue_message(message)
    @queue << message
  end

  public
  def unqueue_message
    @queue.pop
  end

  def connection_handler
    NATS.start({
      :servers => @uris,
    }) do |nc|
      nc.on_reconnect do
        @logger.warn "NATSConnection: Reconnected to server at #{nc.connected_server}"
      end

      nc.on_disconnect do |reason|
        @logger.warn "NATSConnection: Disconnected: #{reason}"
      end

      nc.on_close do
        @logger.warn "NATSConnection: Connection to NATS closed"
      end

      @logger.warn "NATSConnection: Running..."

      EM.tick_loop do
        message = unqueue_message
        nc.publish message[:subject], message[:data]
      end
    end
  end

  def publish(subject, data)
    if !data.is_a? String
      data = data.to_json
    end

    queue_message({
      :subject => subject,
      :data => data,
    })

    block_until_empty
  end
end
