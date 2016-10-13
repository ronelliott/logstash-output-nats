# encoding: utf-8

require "java"
# uncomment these lines to allow jnats to log
require_relative "jars/slf4j-simple-1.7.21.jar"
require_relative "jars/slf4j-api-1.7.21.jar"
require_relative "jars/jnats-0.5.3.jar"
java_import "io.nats.client.ConnectionFactory"

java.lang.System.setProperty "org.slf4j.simpleLogger.defaultLogLevel", "warn"

class NATSConnection
  def initialize(
      uri,
      logger=nil,
      max_reconnect=-1,
      reconnect_time_wait=1000,
      reconnect_buf_size=8*1024*1024,
      publish_timeout=3000)
    super()
    @max_reconnect = max_reconnect
    @publish_timeout = publish_timeout
    @reconnect_time_wait = reconnect_time_wait
    @reconnect_buf_size = reconnect_buf_size
    @logger = logger
    @uri = uri
  end

  public
  def close
    if @conn != nil
      @conn.close
      @conn = nil
    end
  end

  public
  def connect
    if @conn == nil
      cf = ConnectionFactory.new @uri
      cf.setMaxReconnect @max_reconnect
      cf.setReconnectWait @reconnect_time_wait
      cf.setReconnectBufSize @reconnect_buf_size
      @conn = cf.createConnection

      @conn.set_closed_callback do |event|
        @logger.warn "NATSConnection: Connection to #{@uri} closed."
      end

      @conn.set_disconnected_callback do |event|
        @logger.warn "NATSConnection: Disconnected from #{@uri}."
      end

      @conn.set_reconnected_callback do |event|
        @logger.warn "NATSConnection: Reconnected to #{@uri}."
      end
    end
  end

  public
  def reconnect
    close
    connect
  end

  def publish(subject, data)
    connect

    if @conn.closed?
      raise "Socket not open!"
    end

    # really this should be something like !(data.is_a? JavaBytes)
    if data.is_a? String
      data = data.to_java_bytes
    end

    @conn.publish subject, data
    @conn.flush @publish_timeout
  end
end

