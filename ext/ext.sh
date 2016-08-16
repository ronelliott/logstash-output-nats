swig -v -I../cnats/src/install/lib -ruby nats.i && ruby extconf.rb && make && cp nats.bundle ../lib
