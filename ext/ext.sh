swig -v -I../cnats/src/install/include -ruby nats.i && ruby extconf.rb && make && cp nats.bundle ../lib
