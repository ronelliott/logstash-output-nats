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
      @logger.info "NATS: Encoding event"
      @codec.encode event
    rescue Exception => e
      @logger.warn "NATS: Error encoding event", :exception => e, :event => event
    end
  end

  def send_to_nats(event, payload)
    key = event.sprintf @subject
    @logger.info "NATS: publishing event #{key}"

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
    if @socket == nil || @socket.closed?
      do_connect_sequence

      @thread = Thread.new do
        connection_handler
      end
    end

    @socket
  end

  private
  def do_connect_sequence
    @logger.debug("NatsConnection: Connecting to: #{@uri.host}:#{@uri.port}")
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
        if @socket.ready?
          result = @socket.gets

          if result =~ /^PING/
            @socket.write "PONG\r\n"
          end
        end

        # send a buffered message
        message = @queue.pop false
        @socket.write message
        @socket.flush

        # check the response
        result = @socket.gets

        # the response should be "+OK"
        if !(result =~ /^\+OK/)
          @logger.warn "NatsConnection: Could not send event, re-queuing #{result}"
          @logger.debug message
          @queue << message
        end

      rescue Exception => e
        @logger.error("NatsConnection: Error while operating",
          :exception => e,
          :backtrace => e.backtrace)

        if message != nil
          @queue << message
        end

        @socket.close
        @socket = nil
        do_connect_sequence
        retry
      end
    end
  end

  public
  def close
    if @thread != nil
      @thread.join
      @thread = nil
    end

    if @socket != nil
      @socket.close
      @socket = nil
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

  public
  def publish(subject, data)
    connect
    line = generate_publish_data subject, data
    send line
  end

  public
  def send(data)
    @queue << data

    # block until the queue is empty
    until @queue.empty? do
    end
  end
end
