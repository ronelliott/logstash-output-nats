# encoding: utf-8

require "io/wait"
require "json"
require "socket"
require "thread"
require "uri"

# The NATS connection client
class NatsConnection
  def initialize(uri, logger)
    super()
    @cycle_count = 1
    @logger = logger
    @queue = Queue.new
    @uri = URI(uri)
  end

  public
  def block_until_empty
    # block until the queue is empty
    until !has_queued_messages? do
    end
  end

  public
  def connect
    if !connected?
      do_connect_sequence
      create_socket
      create_thread
    end

    @socket
  end

  public
  def connected?
    if @socket != nil then !@socket.closed? else false end
  end

  public
  def connection_handler
    loop do
      message = nil
      @cycle_count += 1

      @logger.info "cycle"

      begin
        # check if the server has sent a PING message
        result = receive
        if message_is_ping? result
          send_pong
        end

        # send a single queued message
        if has_queued_messages?
          # send a buffered message
          message = unqueue_message
          send message

          # check the response
          retry_count = 1
          result = receive
          until (message_is_ok? result) || retry_count >= 10 do
            if message_is_ping? result
              send_pong
            end

            result = receive
            retry_count += 1

            if retry_count >= 10
              queue_message message
            end
          end
        end

        # hacky, but detecting socket closure doesn't seem to be working atm
        # if this is the 25th cycle, send a server PING. reconnect if a PONG
        # is not received within some time.
        if @cycle_count == 25
          @logger.warn "NatsConnection: PINGing server..."
          send generate_ping_data
          result = receive

          retry_count = 1
          until (message_is_pong? result) || retry_count >= 10 do
            sleep 0.01
            result = receive
            retry_count += 1
          end

          if !message_is_pong? result
            @logger.warn "NatsConnection: Server PING failed, reconnecting..."
            do_connect_sequence
          end

          @cycle_count = 1
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

  public
  def close_socket
    if @socket != nil
      @logger.warn "NatsConnection: Closing connection to: #{@uri.host}:#{@uri.port}"
      @socket.close
      @socket = nil
    end
  end

  public
  def close_thread
    if @thread != nil
      @thread.join
      @thread = nil
    end
  end

  public
  def create_socket
    close_socket
    @logger.warn "NatsConnection: Connecting to: #{@uri.host}:#{@uri.port}"
    @socket = TCPSocket.new @uri.host, @uri.port
  end

  public
  def create_thread
    @thread = Thread.new do
      connection_handler
    end
  end

  public
  def do_connect_sequence
    create_socket

    message = receive true # this should be an INFO payload
    if !(message_is_info? message)
      # didn't get an info payload, error out
      @logger.error "NatsConnection: ERROR: #{message}"
      raise "Unexpected state: #{message}"
    end

    send generate_connect_data
    message = receive true # this should be an '+OK' payload

    if !(message_is_ok? message)
      raise "Server connection failed: #{message}"
    end
  end

  public
  def generate_connect_data
    "CONNECT #{{
      :pendantic => false,
      :verbose => true,
      :ssl_required => false,
      :name => "PureRubyNatsPublisher",
      :lang => "ruby",
      :version => "2.0.0",
    }.to_json}\r\n"
  end

  public
  def generate_publish_data(subject, data)
    if !data.is_a? String
      data = data.to_json
    end

    "PUB #{subject} #{data.bytes.to_a.length}\r\n#{data}\r\n"
  end

  public
  def generate_ping_data
    "PING\r\n"
  end

  public
  def generate_pong_data
    "PONG\r\n"
  end

  public
  def queue_message(message)
    @logger.warn "NatsConnection: Re-queuing message: #{message}"
    @queue << message
  end

  public
  def unqueue_message
    @queue.pop false
  end

  public
  def has_queued_messages?
    !@queue.empty?
  end

  public
  def message_is_info?(message)
    message =~ /^INFO /
  end

  public
  def message_is_ok?(message)
    message =~ /^\+OK/
  end

  public
  def message_is_ping?(message)
    message =~ /^PING/
  end

  public
  def message_is_pong?(message)
    message =~ /^PONG/
  end

  public
  def publish(subject, data)
    connect
    message = generate_publish_data subject, data
    queue_message message
    block_until_empty
  end

  public
  def receive(blocking=false)
    @logger.warn "NatsConnection: Receiving -- #{(if !blocking then "non-" else "" end) + "blocking"}"
    @socket.gets if blocking || (!blocking && @socket.ready?)
  end

  public
  def send(data, flush=true)
    @logger.warn "NatsConnection: Sending -- #{(if !flush then "non-" else "" end) + "flushing"} #{data}"
    @socket.write data
    @socket.flush if flush
  end

  public
  def send_pong
    send generate_pong_data
  end
end
