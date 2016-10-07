# encoding: utf-8

require "json"
require "thread"

require "nats/client"

class NATSConnection
  def initialize(uri, logger, reconnect_time_wait=0.1)
    @logger = logger
    @queue = Queue.new
    @reconnect_time_wait = reconnect_time_wait
    @uris = [ uri ]

    Thread.new do
      connection_handler
    end

    super()
  end

  public
  def block_until_empty
    # block until the queue is empty
    until !has_queued_messages? do end
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
    message = nil
    tick_loop = nil

    begin
      NATS.start({
        :servers => @uris,
      }) do |nc|
        nc.on_reconnect do
          @logger.warn "NATSConnection: Reconnected to server at #{nc.connected_server}"
          EM.stop
        end

        nc.on_disconnect do |reason|
          @logger.warn "NATSConnection: Disconnected: #{reason}"
          EM.stop
        end

        nc.on_close do
          @logger.warn "NATSConnection: Connection to NATS closed"
          EM.stop
        end

        @logger.info "NATSConnection: Running..."

        tick_loop = EM.tick_loop do
          message = unqueue_message
          nc.publish message[:subject], message[:data]
        end
      end
    rescue Exception => e
      @logger.error("NATSConnection: Error while operating.",
        :exception => e,
        :backtrace => e.backtrace)
      queue_message(message) if message != nil

      @logger.info "NATSConnection: Stopping EventMachine TickLoop."
      tick_loop.stop if tick_loop != nil

      sleep @reconnect_time_wait
      retry
    end
  end

  def publish(subject, data)
    queue_message({
      :subject => subject,
      :data => !data.is_a?(String) ? data.to_json : data,
    })

    block_until_empty
  end
end
