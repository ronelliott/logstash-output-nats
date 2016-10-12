# encoding: utf-8

require "java"
# uncomment these lines to allow jnats to log
#require_relative "jars/log4j-1.2.17.jar"
#require_relative "jars/slf4j-log4j12-1.7.21.jar"
require_relative "jars/slf4j-nop-1.7.21.jar"
require_relative "jars/slf4j-api-1.7.21.jar"
require_relative "jars/jnats-0.5.3.jar"
java_import "io.nats.client.ConnectionFactory"

class NATSConnection
	def initialize(uri, logger=nil, max_reconnect=-1, reconnect_time_wait=1.0)
		super()
		@max_reconnect = max_reconnect
		@reconnect_time_wait = reconnect_time_wait
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
			@conn = cf.createConnection()

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

		if data.is_a? String
			data = data.to_java_bytes
		end

		@conn.publish subject, data
		@conn.flush
	end
end

