/**
 * ping antidote
 */
function ping() {
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };
  xhttp.open( "GET", 'http://localhost:8088/ping', true);
  xhttp.send();
}

function pingProto() {
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };
  xhttp.open( "GET", 'http://localhost:8088/pingProto', true);
  xhttp.send();
}

/******************Counter Operations******************/

/**
 * create counter
 * @param bType - riak bucket type
 * @param bucket - riak bucket
 * @param key - riak key
 */
function createCounter(bType, bucket, key){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = JSON.stringify({
    bType: bType,
    bucket: bucket,
    key: key
  });

  xhttp.open( "POST", 'http://localhost:8088/createCounter', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

/**
 * delete counter
 * @param bType - riak bucket type
 * @param bucket - riak bucket
 * @param key - riak key
 */
function deleteCounter(bType, bucket, key){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = JSON.stringify({
    bType: bType,
    bucket: bucket,
    key: key
  });

  xhttp.open( "PUT", 'http://localhost:8088/deleteCounter', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

/**
 * fetch counter
 * @param bType - antidote bucket type
 * @param bucket - riak bucket
 * @param key - riak key
 */
function fetchCounter(bType, bucket, key){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = '/' + bType + '/' + bucket + '/' + key;

  xhttp.open( "GET", 'http://localhost:8088/fetchCounter'+data, true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send();
}

function fetchCounterProto(key){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = '/' + key;

  xhttp.open( "GET", 'http://localhost:8088/fetchCounterProto'+data, true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send();
}

/**
 * increment/decrement counter
 * @param bType - riak bucket type
 * @param bucket - riak bucket
 * @param key - riak key
 * @param increment - the value to increment or negative if decrement
 */
function incrementCounter(bType, bucket, key, increment){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = JSON.stringify({
    bType: bType,
    bucket: bucket,
    key: key,
    increment: increment
  });

  xhttp.open( "PUT", 'http://localhost:8088/incrementCounter', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

function incCounter(){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = JSON.stringify({});

  xhttp.open( "PUT", 'http://localhost:8088/incCounter', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

function decCounter(){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = JSON.stringify({});

  xhttp.open( "PUT", 'http://localhost:8088/decCounter', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

/**
 * increment/decrement counter
 * @param bType - doesnt matter
 * @param bucket - doesnt matter
 * @param key - antidote key
 * @param increment - always increments 1
 */
function incrementCounterProto(key, increment){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = JSON.stringify({
    key: key,
    increment: increment
  });

  xhttp.open( "PUT", 'http://localhost:8088/incrementCounterProto', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

function decrementCounterProto(key, decrement){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = JSON.stringify({
    key: key,
    decrement: decrement
  });

  xhttp.open( "PUT", 'http://localhost:8088/decrementCounterProto', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

function fetchObject(bType, bucket, key){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = '/' + bType + '/' + bucket + '/' + key;

  xhttp.open( "GET", 'http://localhost:8088/fetchObject'+data, true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send();
}

function fetchObjectProto(bType, bucket, key){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = '/' + bType + '/' + bucket + '/' + key;

  xhttp.open( "GET", 'http://localhost:8088/fetchObjectProto'+data, true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send();
}

function createObject(bType, bucket, key, value){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = JSON.stringify({
    bType: bType,
    bucket: bucket,
    key: key,
    value: value
  });

  xhttp.open( "POST", 'http://localhost:8088/createObject', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

/******************Set Operations******************/

function fetchSet(bType, bucket, key) {
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = '/' + bType + '/' + bucket + '/' + key;

  xhttp.open( "GET", 'http://localhost:8088/fetchSet'+data, true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send();
}

function fetchSetProto(key) {
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = '/' + key;

  xhttp.open( "GET", 'http://localhost:8088/fetchSetProto'+data, true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send();
}

function createSet(bType, bucket, key) {
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = JSON.stringify({
    bType: bType,
    bucket: bucket,
    key: key
  });

  xhttp.open( "POST", 'http://localhost:8088/createSet', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

function addToSet(bType, bucket, key, additions) {
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = JSON.stringify({
    bType: bType,
    bucket: bucket,
    key: key,
    additions: additions
  });

  xhttp.open( "PUT", 'http://localhost:8088/addToSet', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

/******************Object Operations******************/

function getObjects(key, type) {
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      let response = JSON.parse(xhttp.responseText);
      console.log(response);
      //return xhttp.responseText;
    }
  };

  var data = '/' + key + '/' + type;

  xhttp.open( "GET", 'http://localhost:8088/getObjects'+data, true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send();
}

function updateObjects(key, type, op, elements) {
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      let response = JSON.parse(xhttp.responseText);
      console.log(response);
      //return xhttp.responseText;
    }
  };

  var data = JSON.stringify({
    key: key,
    type: type,
    op: op,
    elements: elements
  });

  xhttp.open( "PUT", 'http://localhost:8088/updateObjects', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

function updateObjectsSnapshot(key, type, op, elements, vClock) {
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      let response = JSON.parse(xhttp.responseText);
      console.log(response);
      return response;
    }
  };

  var data = JSON.stringify({
    key: key,
    type: type,
    op: op,
    elements: elements,
    vClock: vClock
  });

  xhttp.open( "PUT", 'http://localhost:8088/updateObjects', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

/******************Log Operations******************/

function getLogOps(key, type, vClock) {
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      let response = JSON.parse(xhttp.responseText);
      console.log(response);
      //return xhttp.responseText;
    }
  };

  var data = '/' + key + '/' + type + '/' + vClock;

  xhttp.open( "GET", 'http://localhost:8088/getLogOps'+data, true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send();
}

/******************Other Operations******************/

function getLastCommitTimestamp() {
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      return xhttp.responseText;
    }
  };

  xhttp.open( "GET", 'http://localhost:8088/getLastCommitTimestamp', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send();
}