'use strict';

const assert = require('assert');
const Hapi = require('hapi');
const ProtoBuf = require("protobufjs");
const Bert = require('bert-js');
var net = require("net");

// Create a server with a host and port
const server = new Hapi.Server();
server.connection({
  host: 'localhost',
  port: 8000,
  routes: {
    cors: true
  }
});

//riak node ip
const nodes = [
  '127.0.0.1:8087'
];
var client;

server.register(require('inert'), (err) => {

  if (err) {
    throw err;
  }

  //get root page
  server.route({
    method: 'GET',
    path: '/',
    handler: function (request, reply) {
      reply.file('index.html');
    }
  });

  //get index.js script file
  server.route({
    method: 'GET',
    path: '/index.js',
    handler: function (request, reply) {
      reply.file('index.js');
    }
  });

  //ping riak server
  server.route({
    method: 'GET',
    path: '/ping',
    handler: function (request, reply) {
      client.ping(function (err, rslt) {
        if (err) {
          throw new Error(err);
        } else {
          // On success, ping returns true
          assert(rslt === true);
          //console.log('pong');
          return reply('pong');
        }
      });
    }
  });

  //ping antidote server with protocol buffers
  server.route({
    method: 'GET',
    path: '/pingProto',
    handler: function (request, reply) {
      var builder = ProtoBuf.loadProtoFile("protos/fetchObject.proto");
      var riakProto = builder.build("riakProto");
      var RpbPingReq = riakProto.RpbPingReq;
      var RpbPingResp = riakProto.RpbPingResp;

      var encoded = new RpbPingReq().encode().toBuffer();

      //header que é preciso mas que não faço ideia do que faz
      var header = new Buffer(5);
      header.writeUInt8(1, 4);//1 é o código da operação
      header.writeInt32BE(encoded.length + 1, 0);

      var message = {
        protobuf: encoded,
        header: header
      };

      var client = net.connect({port: 8087},
        function() { //'connect' listener
          client.write(message.header);
          client.write(message.protobuf);
        });

      client.on('data', function(data) {
        var headerResp = data.slice(0, 5);
        var protobufResp = data.slice(5, data.length);
        if(RpbPingResp.decode(protobufResp)) {
          return reply('pong');
        }
        client.destroy(); // kill client after server's response
      });

      client.on('close', function() {
        console.log('Connection closed');
      });

      client.on('error', function() {
        console.log('error');
      })

    }
  });



});

/******************Counter Operations******************/

server.route({
  method: 'POST',
  path: '/createCounter',
  handler: function (request, reply) {
    client.storeValue({
        bucketType: request.payload.bType,
        bucket: request.payload.bucket,
        key: request.payload.key,
        value: 0
      },
      function(err, rslt) {
        if (err) {
          throw new Error(err);
        }
        else return reply(rslt);
      }
    );
  }
});

server.route({
  method: 'PUT',
  path: '/deleteCounter',
  handler: function (request, reply) {
    client.deleteValue({
        bucketType: request.payload.bType,
        bucket: request.payload.bucket,
        key: request.payload.key
      },
      function (err, rslt) {
        if (err) {
          throw new Error(err);
        }
        else return reply(rslt);
      });
  }
});

server.route({
  method: 'GET',
  path: '/fetchCounter/{bType}/{bucket}/{key}',
  handler: function (request, reply) {
    client.fetchCounter({
        bucketType: request.params.bType,
        bucket: request.params.bucket,
        key: request.params.key
      },
      function(err, rslt) {
        if (err) {
          throw new Error(err);
        }
        else return reply(rslt);
      }
    );
  }
});

server.route({
  method: 'GET',
  path: '/fetchCounterProto/{bType}/{bucket}/{key}',
  handler: function (request, reply) {
    var ByteBuffer = ProtoBuf.ByteBuffer;
    var Long = ProtoBuf.Long;

    var builder = ProtoBuf.loadProtoFile("protos/antidote.proto");
    var builder2 = ProtoBuf.loadProtoFile("protos/riak.proto");
    var RpbErrorResp = builder2.build("RpbErrorResp");
    var ApbGetCounterResp = builder.build("ApbGetCounterResp");
    var ApbStartTransaction = builder.build("ApbStartTransaction");
    var ApbStartTransactionResp = builder.build("ApbStartTransactionResp");
    var ApbTxnProperties = builder.build("ApbTxnProperties");
    var ApbReadObjects = builder.build("ApbReadObjects");
    var ApbReadObjectsResp = builder.build("ApbReadObjectsResp");
    var ApbBoundObject = builder.build("ApbBoundObject");
    var ApbCommitTransaction = builder.build("ApbCommitTransaction");
    var ApbStaticReadObjects = builder.build("ApbStaticReadObjects");
    var ApbStaticReadObjectsResp = builder.build("ApbStaticReadObjectsResp");

    var startTransaction = new ApbStartTransaction({
      timestamp: ByteBuffer.fromBinary(Bert.encode(Bert.atom("ignore")))
    });

    var antidoteObj = new ApbBoundObject({
      key: ByteBuffer.fromUTF8(request.params.key),
      type: 1,
      bucket: ByteBuffer.fromBinary(Bert.encode(Bert.binary("bucket")))
    });

    var staticRead = new ApbStaticReadObjects({
      transaction: startTransaction,
      objects: antidoteObj
    });

    var encoded = staticRead.encode().toBuffer();
    var header = new Buffer(5);
    header.writeUInt8(123, 4);//1º number is the operation code
    header.writeInt32BE(encoded.length + 1, 0);

    var message = {
      header: header,
      protobuf: encoded
    }

    var client = net.connect({port: 8087},
      function() { //'connect' listener
        client.write(message.header);
        client.write(message.protobuf);
      });

    client.on('data', function(data) {
      console.log("data");
      var headerResp = data.slice(0, 5);
      var respNumber = headerResp.readUInt8(4);
      console.log(respNumber);
      var protobufResp = data.slice(5, data.length);
      var decoded;
      if(respNumber == 0) {
        decoded = RpbErrorResp.decode(protobufResp);
        return reply(decoded.errmsg.toUTF8());
      }
      else if(respNumber == 124) {
        decoded = ApbStartTransactionResp.decode(protobufResp);
        var transactionDescriptor = decoded.transaction_descriptor;
        header = new Buffer(5);
        header.writeUInt8(116, 4);//1º number is the operation code
        header.writeInt32BE(encoded.length + 1, 0);

        var tmp = new ByteBuffer();
        tmp.writeUint32(1);
        tmp.flip();

        var readObj = new ApbReadObjects({
          boundobjects: antidoteObj,
          transaction_descriptor: transactionDescriptor
        });
        encoded = readObj.encode().toBuffer();

        client.write(header);
        client.write(encoded);
      }
      else if(respNumber == 126) {
        decoded = ApbReadObjectsResp.decode(protobufResp);
        console.log(decoded);
        return reply(decoded);
      }
      else if(respNumber == 128) {
        decoded = ApbStaticReadObjectsResp.decode(protobufResp);
        console.log(decoded);
        return reply(decoded);

      }



      //client.destroy(); // kill client after server's response
    });

    client.on('close', function() {
      console.log('Connection closed');
    });

    client.on('error', function() {
      console.log('error');
    });
  }
});

server.route({
  method: 'PUT',
  path: '/incrementCounter',
  handler: function (request, reply) {
    client.updateCounter({
        bucketType: request.payload.bType,
        bucket: request.payload.bucket,
        key: request.payload.key,
        increment: request.payload.increment
      },
      function(err, rslt) {
        if (err) {
          throw new Error(err);
        }
        else return reply(rslt);
      }
    );
  }
});

server.route({
  method: 'PUT',
  path: '/incrementCounterProto',
  handler: function (request, reply) {
    var ByteBuffer = ProtoBuf.ByteBuffer;
    var builder = ProtoBuf.loadProtoFile("protos/antidote.proto");
    var builder2 = ProtoBuf.loadProtoFile("protos/riak.proto");
    var RpbErrorResp = builder2.build("RpbErrorResp");
    var ApbStaticUpdateObjects = builder.build("ApbStaticUpdateObjects");
    var ApbStartTransaction = builder.build("ApbStartTransaction");
    var ApbUpdateOp = builder.build("ApbUpdateOp");
    var ApbBoundObject = builder.build("ApbBoundObject");
    var ApbCounterUpdate = builder.build("ApbCounterUpdate");
    var ApbOperationResp = builder.build("ApbOperationResp");
    var ApbCommitResp = builder.build("ApbCommitResp");

    var startTransaction = new ApbStartTransaction({
      timestamp: ByteBuffer.fromBinary(Bert.encode(Bert.atom("ignore")))
    });

    var antidoteObj = new ApbBoundObject({
      key: ByteBuffer.fromUTF8(request.payload.key),
      type: 1,
      bucket: ByteBuffer.fromBinary(Bert.encode(Bert.binary("bucket")))
    });

    var counterOp = new ApbCounterUpdate({
      optype: 1,
      inc: 1
    });

    var updateOp = new ApbUpdateOp({
      boundobject: antidoteObj,
      optype: 1,
      counterop: counterOp
    });

    var staticUpdate = new ApbStaticUpdateObjects({
      transaction: startTransaction,
      updates: updateOp
    });

    var encoded = staticUpdate.encode().toBuffer();

    var header = new Buffer(5);
    header.writeUInt8(122, 4);//1º number is the operation code
    header.writeInt32BE(encoded.length + 1, 0);

    var message = {
      header: header,
      protobuf: encoded
    }

    var client = net.connect({port: 8087},
      function() { //'connect' listener
        client.write(message.header);
        client.write(message.protobuf);
      });

    client.on('data', function(data) {
      console.log("data");
      var headerResp = data.slice(0, 5);
      var respNumber = headerResp.readUInt8(4);
      console.log(respNumber);
      var protobufResp = data.slice(5, data.length);
      var decoded;
      if(respNumber == 0) {
        decoded = RpbErrorResp.decode(protobufResp);
        return reply(decoded.errmsg.toUTF8());
      }
      else if(respNumber == 111) {
        decoded = ApbOperationResp.decode(protobufResp);
        console.log(decoded);
        return reply(decoded);
      }
      else if(respNumber == 127) {
        decoded = ApbCommitResp.decode(protobufResp);
        console.log(decoded);
        return reply(decoded);
      }

      //client.destroy(); // kill client after server's response
    });

    client.on('close', function() {
      console.log('Connection closed');
    });

    client.on('error', function() {
      console.log('error');
    });

  }
});

server.route({
  method: 'GET',
  path: '/fetchObject/{bType}/{bucket}/{key}',
  handler: function (request, reply) {
    var riakObj = new Riak.Commands.KV.RiakObject();
    riakObj.setContentType('application/riak_counter');
    client.fetchValue({
        bucketType: request.params.bType,
        bucket: request.params.bucket,
        key: request.params.key
      },
      function(err, rslt) {
        if (err) {
          throw new Error(err);
        }
        else {
          //Bert.convention = Bert.ELIXIR;
          //Bert.all_binaries_as_string = true;
          //var tmp = rslt.values[0].getValue();
          //var str = JSON.stringify(tmp);
          //var strObj = JSON.parse(str);
          //var S = Bert.bytes_to_string(strObj.data);

          var riakObj = rslt.values[0];
          var value = riakObj.value.toString("utf-8");
          //var convert = Bert.bytes_to_string(JSON.parse(JSON.stringify(riakObj.value)).data);
          //var convert2 = Bert.decode(convert);
          return reply(value);
        }
      }
    );
  }
});

server.route({
  method: 'GET',
  path: '/fetchObjectProto/{bType}/{bucket}/{key}',
  handler: function (request, reply) {
    var ByteBuffer = ProtoBuf.ByteBuffer;
    var Long = ProtoBuf.Long;
    var builder = ProtoBuf.loadProtoFile("protos/fetchObject.proto");
    var riakProto = builder.build("riakProto");
    var RpbGetReq = riakProto.RpbGetReq;
    var RpbGetResp = riakProto.RpbGetResp;
    var RpbContent = riakProto.RpbContent;

    /*var message = {
      bucket: ByteBuffer.wrap(request.params.bucket),
      key: ByteBuffer.wrap(request.params.key),
      bucketType: ByteBuffer.wrap(request.params.bType)
    };

    var request1 = new riakRequest({
      "bucket": message.bucket,
      "key": message.key,
      "type": message.bucketType
    });*/

    var getRequest = new RpbGetReq({
      bucket : ByteBuffer.fromUTF8(request.params.bucket) ,
      key : ByteBuffer.fromUTF8(request.params.key),
      type : ByteBuffer.fromUTF8(request.params.bType)
    });

    var encoded = getRequest.encode().toBuffer();

    //header que é preciso mas que não faço ideia do que faz
    var header = new Buffer(5);
    header.writeUInt8(9, 4);
    header.writeInt32BE(encoded.length + 1, 0);

    var message = {
      protobuf: encoded,
      header: header
    };

    //var pingReq = new RpbPingReq();

    //var buffer = request.encode();

    var client = net.connect({port: 8087},
      function() { //'connect' listener
        client.write(message.header);
        client.write(message.protobuf);
      });

    client.on('data', function(data) {
      console.log("data");
      var headerResp = data.slice(0, 5);
      var protobufResp = data.slice(5, data.length);
      var decoded = RpbGetResp.decode(protobufResp);
      //var content = RpbContent.decode(decoded.content);
      console.log(decoded.content[0].value.toUTF8());
      return reply(decoded.content[0].value.toUTF8());
      client.destroy(); // kill client after server's response
    });

    client.on('close', function() {
      console.log('Connection closed');
    });

    client.on('error', function() {
      console.log('error');
    })
  }
});

server.route({
  method: 'POST',
  path: '/createObject',
  handler: function (request, reply) {
    var riakObj = new Riak.Commands.KV.RiakObject();
    riakObj.setContentType('text/plain');
    riakObj.setValue(request.payload.value);

    client.storeValue({
        bucketType: request.payload.bType,
        bucket: request.payload.bucket,
        key: request.payload.key,
        value: riakObj
      },
      function(err, rslt) {
        if (err) {
          throw new Error(err);
        }
        else return reply(rslt);
      }
    );
  }
});

/******************Set Operations******************/

server.route({
  method: 'GET',
  path: '/fetchSet/{bType}/{bucket}/{key}',
  handler: function (request, reply) {
    client.fetchSet({
        bucketType: request.params.bType,
        bucket: request.params.bucket,
        key: request.params.key
      },
      function(err, rslt) {
        if (err) {
          throw new Error(err);
        }
        else return reply(rslt);
      }
    );
  }
});

server.route({
  method: 'POST',
  path: '/createSet',
  handler: function (request, reply) {
    client.storeValue({
        bucketType: request.payload.bType,
        bucket: request.payload.bucket,
        key: request.payload.key,
        value: []
      },
      function(err, rslt) {
        if (err) {
          throw new Error(err);
        }
        else return reply(rslt);
      }
    );
  }
});

server.route({
  method: 'PUT',
  path: '/addToSet',
  handler: function (request, reply) {
    client.updateSet({
        bucketType: request.payload.bType,
        bucket: request.payload.bucket,
        key: request.payload.key,
        additions: request.payload.additions
      },
      function(err, rslt) {
        if (err) {
          throw new Error(err);
        }
        else return reply(rslt);
      }
    );
  }
});

// Start the server
server.start((err) => {

  if (err) {
    throw err;
  }
  console.log('Server running at:', server.info.uri);
});
