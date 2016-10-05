# encoding: utf-8

require "logstash/outputs/base"
require "logstash/namespace"

require "io/wait"
require "json"
require "socket"
require "thread"
require "uri"

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

  def register
    @codec.on_event &method(:send_to_nats)
  end

  def get_nats_connection
    if @conn == nil
      @conn = NatsConnection.new @server, @logger
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

# The NATS connection client
class NatsConnection
  def initialize(uri, logger)
    super()
    @uri = URI(uri)
    @logger = logger
    @queue = Queue.new
  end

  public
  def connect
    if !connected?
      do_connect_sequence

      @thread = Thread.new do
        connection_handler
      end
    end

    @socket
  end

  private
  def connected?
    if @socket != nil then !@socket.closed? else false end
  end

  private
  def do_connect_sequence
    close_socket
    @logger.info "NatsConnection: Connecting to: #{@uri.host}:#{@uri.port}"
    @socket = TCPSocket.new @uri.host, @uri.port

    result = @socket.gets # this should be an INFO payload
    if !(result =~ /^INFO /)
      # didn't get an info payload, error out
      @logger.error "NatsConnection: ERROR: #{@socket.gets}"
      raise "Unexpected state: #{result}"
    end

    @socket.write generate_connect_data
    result = @socket.gets # this should be an '+OK' payload

    if !(result =~ /^\+OK/)
      raise "Server connection failed: #{result}"
    end
  end

  private
  def connection_handler
    loop do
      message = nil

      begin
        # check if the server has sent a PING message
        result = receive
        if result =~ /^PING/
          send "PONG\r\n"
        end

        if has_queued_messages?
          # send a buffered message
          message = unqueue_message
          send message

          # check the response
          result = receive
          until result =~ /^\+OK/ do
            if result =~ /^PING/
              send "PONG\r\n"
            end

            result = receive
          end
        end

      rescue Exception => e
        @logger.error("NatsConnection: Error while operating",
          :exception => e,
          :backtrace => e.backtrace)

        if message != nil
          queue_message message
        end

        do_connect_sequence
        retry
      end
    end
  end

  public
  def close
    close_thread
    close_socket
  end

  private
  def close_socket
    if @socket != nil
      @socket.close
      @socket = nil
    end
  end

  private
  def close_thread
    if @thread != nil
      @thread.join
      @thread = nil
    end
  end

  public
  def generate_connect_data
    opts = {
      :pendantic => false,
      :verbose => true,
      :ssl_required => false,
      :name => "PureRubyNatsPublisher",
      :lang => "ruby",
      :version => "2.0.0",
    }

    "CONNECT #{opts.to_json}\r\n"
  end

  public
  def generate_publish_data(subject, data)
    if !data.is_a? String
      data = data.to_json
    end

    "PUB #{subject} #{data.bytes.to_a.length}\r\n#{data}\r\n"
  end

  private
  def queue_message(message)
    @queue << message
  end

  private
  def unqueue_message
    @queue.pop false
  end

  private
  def has_queued_messages?
    !@queue.empty?
  end

  public
  def publish(subject, data)
    connect
    message = generate_publish_data subject, data
    queue_message message

    # block until the queue is empty
    until !has_queued_messages? do
    end
  end

  private
  def receive
    @socket.gets if @socket.ready?
  end

  private
  def send(data)
    @socket.write data
    @socket.flush
  end
end
