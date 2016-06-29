'use strict';

const assert = require('assert');
const Hapi = require('hapi');
const ProtoBuf = require("protobufjs");
const Bert = require('bert-js');
var net = require("net");
//var zmq = require('zmq'), sock = zmq.socket('pub');

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
var ApbStaticUpdateObjects = builder.build("ApbStaticUpdateObjects");
var ApbUpdateOp = builder.build("ApbUpdateOp");
var ApbCounterUpdate = builder.build("ApbCounterUpdate");
var ApbOperationResp = builder.build("ApbOperationResp");
var ApbCommitResp = builder.build("ApbCommitResp");

var opQeueu = [];
var lastCommitTimestamp;

// Create a server with a host and port
const server = new Hapi.Server();
server.connection({
  host: 'localhost',
  port: 8088,
  routes: {
    cors: true
  }
});

//riak node ip
const nodes = [
  '127.0.0.1:8087'
];

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
    path: '/antidoteClient.js',
    handler: function (request, reply) {
      reply.file('antidoteClient.js');
    }
  });

  server.route({
    method: 'GET',
    path: '/JSMQ.js',
    handler: function (request, reply) {
      reply.file('JSMQ.js');
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

      let client = net.connect({port: 8087},
        function() { //'connect' listener
          client.write(message.header);
          client.write(message.protobuf);
        });

      client.on('data', function(data) {
        var headerResp = data.slice(0, 5);
        let respNumber = headerResp.readUInt8(4);
        console.log('data ' + respNumber);
        var protobufResp = data.slice(5, data.length);
        if(respNumber == 2) {
          return reply('pong');
        }
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
  method: 'GET',
  path: '/fetchCounterProto/{key}',
  handler: function (request, reply) {

    var startTransaction = new ApbStartTransaction({
      timestamp: ByteBuffer.fromBinary(Bert.encode(Bert.atom("ignore")))
    });

    var antidoteObj = new ApbBoundObject({
      key: ByteBuffer.fromUTF8(request.params.key),
      type: 0,
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

    let client = net.connect({port: 8087});
    client.write(message.header);
    client.write(message.protobuf);

    client.on('data', function(data) {
      var headerResp = data.slice(0, 5);
      var respNumber = headerResp.readUInt8(4);
      console.log('data ' + respNumber);
      var protobufResp = data.slice(5, data.length);
      var decoded;
      if(respNumber == 0) {
        decoded = RpbErrorResp.decode(protobufResp);
        reply(decoded.errmsg.toUTF8());
      }
      else if(respNumber == 128) {
        decoded = ApbStaticReadObjectsResp.decode(protobufResp);
        reply(decoded);
      }
      client.destroy();
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
  path: '/incCounter',
  handler: function (request, reply) {
    opQeueu.push("inc");
    reply('inc added to queue');
  }
});

server.route({
  method: 'PUT',
  path: '/decCounter',
  handler: function (request, reply) {
    opQeueu.push("dec");
    reply('dec added to queue');
  }
});

server.route({
  method: 'PUT',
  path: '/incrementCounterProto',
  handler: function (request, reply) {
    var startTransaction = new ApbStartTransaction({
      timestamp: ByteBuffer.fromBinary(Bert.encode(Bert.atom("ignore")))
    });

    var antidoteObj = new ApbBoundObject({
      key: ByteBuffer.fromUTF8(request.payload.key),
      type: 0,
      bucket: ByteBuffer.fromBinary(Bert.encode(Bert.binary("bucket")))
    });

    var counterOp = new ApbCounterUpdate({
      optype: 1,
      inc: request.payload.increment
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

    let client = net.connect({port: 8087});
    client.write(message.header);
    client.write(message.protobuf);

    client.on('data', function(data) {
      var headerResp = data.slice(0, 5);
      var respNumber = headerResp.readUInt8(4);
      console.log('data ' + respNumber);
      var protobufResp = data.slice(5, data.length);
      var decoded;
      if(respNumber == 0) {
        decoded = RpbErrorResp.decode(protobufResp);
        return decoded.errmsg.toUTF8();
      }
      else if(respNumber == 111) {
        decoded = ApbOperationResp.decode(protobufResp);
      }
      else if(respNumber == 127) {

        decoded = ApbCommitResp.decode(protobufResp);
        //console.log(decoded);
      }
      else if(respNumber == 128) {
        decoded = ApbStaticReadObjectsResp.decode(protobufResp);
      }
      //console.log(decoded);
      client.destroy();
      return reply(decoded);
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
  path: '/decrementCounterProto',
  handler: function (request, reply) {
    var startTransaction = new ApbStartTransaction({
      timestamp: ByteBuffer.fromBinary(Bert.encode(Bert.atom("ignore")))
    });

    var antidoteObj = new ApbBoundObject({
      key: ByteBuffer.fromUTF8(request.payload.key),
      type: 0,
      bucket: ByteBuffer.fromBinary(Bert.encode(Bert.binary("bucket")))
    });

    var counterOp = new ApbCounterUpdate({
      optype: 2,
      dec: request.payload.decrement
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
    };

    let client = net.connect({port: 8087});
    client.write(message.header);
    client.write(message.protobuf);

    client.on('data', function(data) {
      var headerResp = data.slice(0, 5);
      var respNumber = headerResp.readUInt8(4);
      console.log('data ' + respNumber);
      var protobufResp = data.slice(5, data.length);
      var decoded;
      if(respNumber == 0) {
        decoded = RpbErrorResp.decode(protobufResp);
        return decoded.errmsg.toUTF8();
      }
      else if(respNumber == 111) {
        decoded = ApbOperationResp.decode(protobufResp);
      }
      else if(respNumber == 127) {

        decoded = ApbCommitResp.decode(protobufResp);
        //console.log(decoded);
      }
      else if(respNumber == 128) {
        decoded = ApbStaticReadObjectsResp.decode(protobufResp);
      }
      //console.log(decoded);
      client.destroy();
      return reply(decoded);
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
  path: '/fetchObjectProto/{bType}/{bucket}/{key}',
  handler: function (request, reply) {
    var ByteBuffer = ProtoBuf.ByteBuffer;
    var builder = ProtoBuf.loadProtoFile("protos/riak_kv.proto");
    var builder2 = ProtoBuf.loadProtoFile("protos/riak.proto");
    var RpbErrorResp = builder2.build("RpbErrorResp");
    var RpbGetReq = builder.build("RpbGetReq");
    var RpbGetResp = builder.build("RpbGetResp");

    var getRequest = new RpbGetReq({
      bucket : ByteBuffer.fromBinary(Bert.encode(Bert.binary("bucket"))) ,
      key : ByteBuffer.fromUTF8(request.params.key),
      type : ByteBuffer.fromUTF8(request.params.bType)
    });

    var encoded = getRequest.encode().toBuffer();

    //header que é preciso mas que não faço ideia do que faz
    var header = new Buffer(5);
    header.writeUInt8(9, 4);//1º number is the operation code
    header.writeInt32BE(encoded.length + 1, 0);


    var message = {
      protobuf: encoded,
      header: header
    };

    let client = net.connect({port: 8087},
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

      if(respNumber == 0) {
        decoded = RpbErrorResp.decode(protobufResp);
        return reply(decoded.errmsg.toUTF8());
      }
      else if(respNumber == 10) {
        var decoded = RpbGetResp.decode(protobufResp);
        //var content = RpbContent.decode(decoded.content);
        console.log(decoded.content[0].value.toUTF8());
      }

      return reply(decoded.content[0].value.toUTF8());
    });

    client.on('close', function() {
      console.log('Connection closed');
    });

    client.on('error', function() {
      console.log('error');
    })
  }
});

/******************Set Operations******************/

server.route({
  method: 'GET',
  path: '/fetchSetProto/{key}',
  handler: function (request, reply) {
    var ByteBuffer = ProtoBuf.ByteBuffer;
    var builder = ProtoBuf.loadProtoFile("protos/antidote.proto");
    var builder2 = ProtoBuf.loadProtoFile("protos/riak.proto");
    var RpbErrorResp = builder2.build("RpbErrorResp");
    var ApbStaticReadObjects = builder.build("ApbStaticReadObjects");
    var ApbStartTransaction = builder.build("ApbStartTransaction");
    var ApbBoundObject = builder.build("ApbBoundObject");
    var ApbStaticReadObjectsResp = builder.build("ApbStaticReadObjectsResp");

    var startTransaction = new ApbStartTransaction({
      timestamp: ByteBuffer.fromBinary(Bert.encode(Bert.atom("ignore")))
    });

    var antidoteObj = new ApbBoundObject({
      key: ByteBuffer.fromUTF8(request.params.key),
      type: 2,
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
    };

    let client = net.connect({port: 8087},
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
      else if(respNumber == 126) {
        decoded = ApbReadObjectsResp.decode(protobufResp);
      }
      else if(respNumber == 128) {
        decoded = ApbStaticReadObjectsResp.decode(protobufResp);
        return reply(decoded.objects.objects[0].set.value);
      }
      //console.log(decoded);
      return reply(decoded);
    });

    client.on('close', function() {
      console.log('Connection closed');
    });

    client.on('error', function() {
      console.log('error');
    });

  }
});


/****************************************************/


function incCounter(key, increment) {

  var startTransaction = new ApbStartTransaction({
    timestamp: ByteBuffer.fromBinary(Bert.encode(Bert.atom("ignore")))
  });

  var antidoteObj = new ApbBoundObject({
    key: ByteBuffer.fromUTF8(key),
    type: 0,
    bucket: ByteBuffer.fromBinary(Bert.encode(Bert.binary("bucket")))
  });

  var counterOp = new ApbCounterUpdate({
    optype: 1,
    inc: increment
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

  client.write(message.header);
  client.write(message.protobuf);

}

function decCounter(key) {

  var startTransaction = new ApbStartTransaction({
    timestamp: ByteBuffer.fromBinary(Bert.encode(Bert.atom("ignore")))
  });

  var antidoteObj = new ApbBoundObject({
    key: ByteBuffer.fromUTF8(key),
    type: 0,
    bucket: ByteBuffer.fromBinary(Bert.encode(Bert.binary("bucket")))
  });

  var counterOp = new ApbCounterUpdate({
    optype: 2,
    dec: 1
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

  client.write(message.header);
  client.write(message.protobuf);
}

function propagateChanges() {
  if (opQeueu.length != 0) {
    var op = opQeueu.shift();
    switch (op) {
      case "inc":
        console.log('inc');
        incCounter("myPnCounter", propagateChanges());
        break;
      case "dec":
        console.log('dec');
        decCounter("myPnCounter", propagateChanges());
        break;
      default:
        console.error('unvalid op ' + op);
    }
  }
  //if(opQeueu.length != 0) {propagateChanges();}
}

function receiveData() {
  client.on('data', function(data) {
    console.log("data");
    var headerResp = data.slice(0, 5);
    var respNumber = headerResp.readUInt8(4);
    console.log(respNumber);
    var protobufResp = data.slice(5, data.length);
    var decoded;
    if(respNumber == 0) {
      decoded = RpbErrorResp.decode(protobufResp);
      return decoded.errmsg.toUTF8();
    }
    else if(respNumber == 111) {
      decoded = ApbOperationResp.decode(protobufResp);
    }
    else if(respNumber == 127) {

      decoded = ApbCommitResp.decode(protobufResp);
      console.log(decoded);
    }
    else if(respNumber == 128) {
      decoded = ApbStaticReadObjectsResp.decode(protobufResp);
    }
    return decoded;
  });

  client.on('close', function() {
    console.log('Connection closed');
  });

  client.on('error', function() {
    console.log('error');
  });
}


/******************Object Operations******************/

server.route({
  method: 'GET',
  path: '/readObjects/{key}/{type}',
  handler: function (request, reply) {
    var ByteBuffer = ProtoBuf.ByteBuffer;
    var builder = ProtoBuf.loadProtoFile("protos/antidote.proto");
    var builder2 = ProtoBuf.loadProtoFile("protos/riak.proto");
    var RpbErrorResp = builder2.build("RpbErrorResp");
    var ApbBoundObject = builder.build("ApbBoundObject");
    var ApbObjectResp = builder.build("ApbObjectResp");
    var ApbStartTransaction = builder.build("ApbStartTransaction");
    var ApbStartTransactionResp = builder.build("ApbStartTransactionResp");
    var ApbVectorclock = builder.build("ApbVectorclock");
    var ApbReadObjects = builder.build("ApbReadObjects");
    var ApbReadObjectsResp = builder.build("ApbReadObjectsResp");
    var ApbCommitTransaction = builder.build("ApbCommitTransaction");
    var ApbCommitResp = builder.build("ApbCommitResp");
    let descriptor;

    let timestamp = new ApbVectorclock({
      value: ByteBuffer.fromBinary(Bert.encode(Bert.atom("ignore")))
    });

    let startTransaction = new ApbStartTransaction({
      timestamp: timestamp
    });

    /*var antidoteObj = new ApbBoundObject({
     key: ByteBuffer.fromUTF8(request.params.key),
     type: parseInt(request.params.type),
     bucket: ByteBuffer.fromBinary(Bert.encode(Bert.binary("bucket")))
     });

     let getObjectsReq = new ApbGetObjects({
     boundobjects: antidoteObj
     });*/

    var encoded = startTransaction.encode().toBuffer();

    var header = new Buffer(5);
    header.writeUInt8(119, 4);//1st number is the operation code
    header.writeInt32BE(encoded.length + 1, 0);

    var message = {
      header: header,
      protobuf: encoded
    };

    let client = net.connect({port: 8087},
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
      else if(respNumber == 128) {
        decoded = ApbStaticReadObjectsResp.decode(protobufResp);
        return reply(decoded.objects.objects[0].set.value);
      }
      else if(respNumber == 131) {
        decoded = ApbGetObjectsResp.decode(protobufResp);
        return reply(decoded.objects[0].value.toUTF8());
      }
      else if(respNumber == 124) {
        decoded = ApbStartTransactionResp.decode(protobufResp);
        console.log(decoded.success);
        if(decoded.success) {
          descriptor = decoded.transaction_descriptor;
          //console.log(descriptor);

          let antidoteObj = new ApbBoundObject({
            key: ByteBuffer.fromUTF8(request.params.key),
            type: parseInt(request.params.type),
            bucket: ByteBuffer.fromBinary(Bert.encode(Bert.binary("bucket")))
          });

          let readObjects = new ApbReadObjects({
            boundobjects: antidoteObj,
            transaction_descriptor: descriptor
          });

          encoded = readObjects.encode().toBuffer();

          header = new Buffer(5);
          header.writeUInt8(116, 4);//1º number is the operation code
          header.writeInt32BE(encoded.length + 1, 0);

          message = {
            header: header,
            protobuf: encoded
          };

          client.write(message.header);
          client.write(message.protobuf);

        }
      }
      else if(respNumber == 126) {
        decoded = ApbReadObjectsResp.decode(protobufResp);
        console.log(decoded);

        let commit = new ApbCommitTransaction({
          transaction_descriptor: descriptor
        });

        encoded = commit.encode().toBuffer();

        header = new Buffer(5);
        header.writeUInt8(121, 4);//1º number is the operation code
        header.writeInt32BE(encoded.length + 1, 0);

        message = {
          header: header,
          protobuf: encoded
        };

        client.write(message.header);
        client.write(message.protobuf);

        return reply(decoded.objects[0].set.value.toUTF8());
      }
      else if(respNumber == 127) {
        decoded = ApbCommitResp.decode(protobufResp);
        lastCommitTimestamp = decoded.commit_time;
        client.destroy();
        //return reply(decoded);
      }
      //console.log(decoded);
      //return reply(decoded);
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
  path: '/getObjects/{key}/{type}',
  handler: function (request, reply) {
    var ByteBuffer = ProtoBuf.ByteBuffer;
    var builder = ProtoBuf.loadProtoFile("protos/antidote.proto");
    var builder2 = ProtoBuf.loadProtoFile("protos/riak.proto");
    var RpbErrorResp = builder2.build("RpbErrorResp");
    var ApbBoundObject = builder.build("ApbBoundObject");
    var ApbGetObjects = builder.build("ApbGetObjects");
    var ApbGetObjectsResp = builder.build("ApbGetObjectsResp");
    var ApbJsonRequest = builder.build("ApbJsonRequest");
    var ApbJsonResp = builder.build("ApbJsonResp");

    /*var antidoteObj = new ApbBoundObject({
      key: ByteBuffer.fromUTF8(request.params.key),
      type: parseInt(request.params.type),
      bucket: ByteBuffer.fromBinary(Bert.encode(Bert.binary("bucket")))
    });

    let getObjectsReq = new ApbGetObjects({
      boundobjects: antidoteObj
    });*/

    let jsonGetReq = {
        get_objects: [{
          boundobjects: [{
            bound_object: [
              request.params.key,
              request.params.type,
              "bucket"
            ]
          }]
        }]
      };

    let jsonReq = new ApbJsonRequest({
      value: ByteBuffer.fromUTF8(JSON.stringify(jsonGetReq))
    });

    var encoded = jsonReq.encode().toBuffer();

    var header = new Buffer(5);
    header.writeUInt8(135, 4);//1st number is the operation code
    header.writeInt32BE(encoded.length + 1, 0);

    var message = {
      header: header,
      protobuf: encoded
    };

    let client = net.connect({port: 8087},
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
      else if(respNumber == 126) {
        decoded = ApbReadObjectsResp.decode(protobufResp);
      }
      else if(respNumber == 128) {
        decoded = ApbStaticReadObjectsResp.decode(protobufResp);
        return reply(decoded.objects.objects[0].set.value);
      }
      else if(respNumber == 131) {
        decoded = ApbGetObjectsResp.decode(protobufResp);
        //return reply(decoded.objects[0].value.toUTF8());
        return reply(decoded);
      }
      else if(respNumber == 136) {
        decoded = ApbJsonResp.decode(protobufResp);
        return reply(decoded.value.toUTF8());
      }

      //console.log(decoded);
      return reply(decoded);
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
  path: '/updateObjects',
  handler: function (request, reply) {
    var ByteBuffer = ProtoBuf.ByteBuffer;
    var builder = ProtoBuf.loadProtoFile("protos/antidote.proto");
    var builder2 = ProtoBuf.loadProtoFile("protos/riak.proto");
    var RpbErrorResp = builder2.build("RpbErrorResp");
    var ApbStartTransaction = builder.build("ApbStartTransaction");
    var ApbVectorclock = builder.build("ApbVectorclock");
    var ApbBoundObject = builder.build("ApbBoundObject");
    var ApbUpdateObjects = builder.build("ApbUpdateObjects");
    var ApbUpdateOp = builder.build("ApbUpdateOp");
    var ApbSetUpdate = builder.build("ApbSetUpdate");
    var ApbCommitTransaction = builder.build("ApbCommitTransaction");
    var ApbStartTransactionResp = builder.build("ApbStartTransactionResp");
    var ApbCommitResp = builder.build("ApbCommitResp");
    var ApbJsonRequest = builder.build("ApbJsonRequest");
    var ApbJsonResp = builder.build("ApbJsonResp");
    let descriptor;

    var vClock = new ApbVectorclock({
      value: ByteBuffer.fromBinary(Bert.encode(Bert.atom("ignore")))
    });

    var startTransaction = new ApbStartTransaction({
      timestamp: vClock
    });

    let startTX;
    if(request.payload.vClock){
      console.log("aqui");
      startTX = {
        start_transaction: [
          {
            vectorclock: [
              {
                dcid_and_time: [
                  {
                    dcid: [
                      request.payload.vClock[0],
                      request.payload.vClock[1],
                      request.payload.vClock[2],
                      request.payload.vClock[3]
                    ]
                  },
                  request.payload.vClock[4]
                ]
              }
            ]
          },
          {
            txn_properties: [
              [
                "update_clock",
                false
              ]
            ]
          }
        ]
      };
    }
    else {
      console.log("ali");
      startTX = {
        start_transaction: [
          "ignore",
          {
            txn_properties: []
          }
        ]
      };
    }

    let startTXJson = new ApbJsonRequest({
      value: ByteBuffer.fromUTF8(JSON.stringify(startTX))
    });

    var encoded = startTXJson.encode().toBuffer();

    var header = new Buffer(5);
    header.writeUInt8(135, 4);//1º number is the operation code
    header.writeInt32BE(encoded.length + 1, 0);

    var message = {
      header: header,
      protobuf: encoded
    };

    let client = net.connect({port: 8087});
    client.write(message.header);
    client.write(message.protobuf);

    client.on('data', function(data) {
      var headerResp = data.slice(0, 5);
      var respNumber = headerResp.readUInt8(4);
      console.log('data ' + respNumber);
      var protobufResp = data.slice(5, data.length);
      var decoded;
      if(respNumber == 0) {
        decoded = RpbErrorResp.decode(protobufResp);
        console.log(decoded.errmsg.toUTF8());
        client.destroy();
        return decoded.errmsg.toUTF8();
      }
      else if(respNumber == 111) {
        decoded = ApbOperationResp.decode(protobufResp);
        console.log(decoded);

        let commit = new ApbCommitTransaction({
          transaction_descriptor: descriptor
        });

        encoded = commit.encode().toBuffer();

        header = new Buffer(5);
        header.writeUInt8(121, 4);//1º number is the operation code
        header.writeInt32BE(encoded.length + 1, 0);

        message = {
          header: header,
          protobuf: encoded
        };

        client.write(message.header);
        client.write(message.protobuf);

      }
      else if(respNumber == 127) {
        decoded = ApbCommitResp.decode(protobufResp);
        lastCommitTimestamp = decoded.commit_time;
        client.destroy();
        return reply(decoded);
      }
      else if(respNumber == 128) {
        decoded = ApbStaticReadObjectsResp.decode(protobufResp);
      }
      else if(respNumber == 124) {
        decoded = ApbStartTransactionResp.decode(protobufResp);
        console.log(decoded.success);
        if(decoded.success) {
          descriptor = decoded.transaction_descriptor;
          //console.log(descriptor);

          let setUpdate = new ApbSetUpdate({
            optype: parseInt(request.payload.op),
            adds: ByteBuffer.fromBinary(Bert.encode(Bert.binary(request.payload.elements))),
            rems: ByteBuffer.fromBinary(Bert.encode(Bert.binary(request.payload.elements)))
          });

          let antidoteObj = new ApbBoundObject({
            key: ByteBuffer.fromUTF8(request.payload.key),
            type: parseInt(request.payload.type),
            bucket: ByteBuffer.fromBinary(Bert.encode(Bert.binary("bucket")))
          });

          let updateOp = new ApbUpdateOp({
            boundobject: antidoteObj,
            optype: 2,//type update, 1=counter, 2=set
            setop: setUpdate
          });

          let updateObjects = new ApbUpdateObjects({
            updates: updateOp,
            transaction_descriptor: descriptor
          });

          encoded = updateObjects.encode().toBuffer();

          header = new Buffer(5);
          header.writeUInt8(118, 4);//1º number is the operation code
          header.writeInt32BE(encoded.length + 1, 0);

          message = {
            header: header,
            protobuf: encoded
          };

          client.write(message.header);
          client.write(message.protobuf);

        }
      }
      else if(respNumber == 136) {
        decoded = JSON.parse(ApbJsonResp.decode(protobufResp).value.toUTF8());
        console.log(JSON.stringify(decoded));
        if(decoded.success.start_transaction_resp){
          descriptor = decoded.success.start_transaction_resp.txid;
          console.log(descriptor);

          let updateObjects = {
            update_objects:
            [
              [
                {
                  update_op: [
                    {
                      bound_object: [
                        request.payload.key,
                        request.payload.type,
                        "bucket"
                      ]
                    },
                    request.payload.op,
                    {json_value: request.payload.elements}
                  ]
                }
              ],
              {
                txid: [
                  {json_value: descriptor[0].json_value},
                  descriptor[1]
                ]
              }
            ]
          };

          let updateJSON = new ApbJsonRequest({
            value: ByteBuffer.fromUTF8(JSON.stringify(updateObjects))
          });

          encoded = updateJSON.encode().toBuffer();

          header = new Buffer(5);
          header.writeUInt8(135, 4);//1º number is the operation code
          header.writeInt32BE(encoded.length + 1, 0);

          message = {
            header: header,
            protobuf: encoded
          };

          client.write(message.header);
          client.write(message.protobuf);
        }
        else if(decoded.success == 'ok'){
          let commit = {
            commit_transaction: {
              txid: [
                {json_value: descriptor[0].json_value},
                descriptor[1]
              ]
            }
          };

          let commitJSON = new ApbJsonRequest({
            value: ByteBuffer.fromUTF8(JSON.stringify(commit))
          });

          encoded = commitJSON.encode().toBuffer();

          header = new Buffer(5);
          header.writeUInt8(135, 4);//1º number is the operation code
          header.writeInt32BE(encoded.length + 1, 0);

          message = {
            header: header,
            protobuf: encoded
          };

          client.write(message.header);
          client.write(message.protobuf);
        }
        else if(decoded.success.commit_resp){
          return reply(decoded);
        }
        else {return reply(decoded);}
      }
    });

    client.on('close', function() {
      console.log('Connection closed');
    });

    client.on('error', function() {
      console.log('error');
    });
  }
});


/******************Object Operations******************/

server.route({
  method: 'GET',
  path: '/getLogOps/{key}/{type}/{vClock}',
  handler: function (request, reply) {
    var ByteBuffer = ProtoBuf.ByteBuffer;
    var builder = ProtoBuf.loadProtoFile("protos/antidote.proto");
    var builder2 = ProtoBuf.loadProtoFile("protos/riak.proto");
    var RpbErrorResp = builder2.build("RpbErrorResp");
    var ApbGetLogOperations = builder.build("ApbGetLogOperations");
    var ApbGetLogOperationsResp = builder.build("ApbGetLogOperationsResp");
    var ApbLogOperationResp = builder.build("ApbLogOperationResp");
    var ApbJsonRequest = builder.build("ApbJsonRequest");
    var ApbJsonResp = builder.build("ApbJsonResp");


    /*let antidoteObj = new ApbBoundObject({
      key: ByteBuffer.fromUTF8(request.params.key),
      type: parseInt(request.params.type),
      bucket: ByteBuffer.fromBinary(Bert.encode(Bert.binary("bucket")))
    });

    let getLogOps = new ApbGetLogOperations({
      timestamp: lastCommitTimestamp,
      boundobjects: antidoteObj
    });*/

    let vClock = JSON.parse(request.params.vClock);

    let getLogOps = {
      get_log_operations: [
        {
          timestamps: [
            {
              vectorclock: [
                {
                  dcid_and_time: [
                    {
                      dcid: [
                        vClock[0],
                        vClock[1],
                        vClock[2],
                        vClock[3]
                      ]
                    },
                    vClock[4]
                  ]
                }
              ]
            }
          ]
        },
        {
          boundobjects: [
            {
              bound_object: [
                request.params.key,
                request.params.type,
                "bucket"
              ]
            }
          ]
        }
      ]
    };

    let getLogOpsJSON = new ApbJsonRequest({
      value: ByteBuffer.fromUTF8(JSON.stringify(getLogOps))
    });

    var encoded = getLogOpsJSON.encode().toBuffer();

    var header = new Buffer(5);
    header.writeUInt8(135, 4);//1st number is the operation code
    header.writeInt32BE(encoded.length + 1, 0);

    var message = {
      header: header,
      protobuf: encoded
    };

    let client = net.connect({port: 8087},
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
      else if(respNumber == 134) {
        decoded = ApbGetLogOperationsResp.decode(protobufResp);
        return reply(decoded.objects[0].value.toUTF8());
      }
      else if(respNumber == 136) {
        decoded = JSON.parse(ApbJsonResp.decode(protobufResp).value.toUTF8());
        console.log(decoded);
        return reply(decoded);
      }
      //console.log(decoded);
      return reply(decoded);
    });

    client.on('close', function() {
      console.log('Connection closed');
    });

    client.on('error', function() {
      console.log('error');
    });

  }
});

/******************Other Operations******************/

server.route({
  method: 'GET',
  path: '/getLastCommitTimestamp',
  handler: function (request, reply) {
    reply(lastCommitTimestamp);
  }
});


// Start the server
server.start((err) => {

  if (err) {
    throw err;
  }
  console.log('Server running at:', server.info.uri);


  //setInterval(propagateChanges ,7000);
  //sock.bindSync('tcp://127.0.0.1:3000');
  //console.log('Publisher bound to port 3000');

  /*setInterval(function(){
    console.log('sending a multipart message envelope');
    sock.send(['kitty cats', 'meow!']);
  }, 3000);*/
});
