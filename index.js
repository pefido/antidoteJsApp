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
  xhttp.open( "GET", 'http://localhost:8000/ping', true);
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
  xhttp.open( "GET", 'http://localhost:8000/pingProto', true);
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

  xhttp.open( "POST", 'http://localhost:8000/createCounter', true);
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

  xhttp.open( "PUT", 'http://localhost:8000/deleteCounter', true);
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

  xhttp.open( "GET", 'http://localhost:8000/fetchCounter'+data, true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send();
}

function fetchCounterProto(bType, bucket, key){
  var xhttp = new XMLHttpRequest();
  xhttp.onreadystatechange = function() {
    if (xhttp.readyState == 4 && xhttp.status == 200) {
      console.log(xhttp.responseText);
      //return xhttp.responseText;
    }
  };

  var data = '/' + bType + '/' + bucket + '/' + key;

  xhttp.open( "GET", 'http://localhost:8000/fetchCounterProto'+data, true);
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

  xhttp.open( "PUT", 'http://localhost:8000/incrementCounter', true);
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

  xhttp.open( "PUT", 'http://localhost:8000/incCounter', true);
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

  xhttp.open( "PUT", 'http://localhost:8000/decCounter', true);
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
function incrementCounterProto(bType, bucket, key, increment){
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

  xhttp.open( "PUT", 'http://localhost:8000/incrementCounterProto', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}

function decrementCounterProto(bType, bucket, key, increment){
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

  xhttp.open( "PUT", 'http://localhost:8000/decrementCounterProto', true);
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

  xhttp.open( "GET", 'http://localhost:8000/fetchObject'+data, true);
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

  xhttp.open( "GET", 'http://localhost:8000/fetchObjectProto'+data, true);
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

  xhttp.open( "POST", 'http://localhost:8000/createObject', true);
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

  xhttp.open( "GET", 'http://localhost:8000/fetchSet'+data, true);
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

  xhttp.open( "GET", 'http://localhost:8000/fetchSetProto'+data, true);
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

  xhttp.open( "POST", 'http://localhost:8000/createSet', true);
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

  xhttp.open( "PUT", 'http://localhost:8000/addToSet', true);
  xhttp.setRequestHeader('Content-Type', 'application/json; charset=UTF-8');
  xhttp.send(data);
}