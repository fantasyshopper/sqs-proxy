/**
 *  Simple SQS proxy listening on a TCP port.
 */
var tcp = require('net'), http = require('http'), aws = require('aws2js');

var config = require('../conf/config.json'),
  AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID || null,
  AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY || null,
  AWS_SECURITY_TOKEN = process.env.AWS_SECURITY_TOKEN || null,
  IAM_REFRESH_PERIOD = 20*60*1000,
  IAM_ROLE = '',
  IAM_UPDATE = +(new Date()),
  RUNNING = false,
  sqs = aws.load('sqs');

  sqs.setRegion(config.region||"eu-west-1");

var DEBUG = process.argv[2] && process.argv[2] === "debug";


if(!AWS_ACCESS_KEY_ID){
    //Use IAM to get credentials.
    getIAMRole(getIAMCredentials);
}else{
    sqs.setCredentials(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SECURITY_TOKEN);
    start();
}

function getIAMRole(fn){
    var req = http.request({
        'host': '169.254.169.254',
        'path': '/latest/meta-data/iam/security-credentials/'
    }, function(res){
        res.setEncoding('utf8');
        var found = false;
        res.on('data', function(data){
            if(found){ return; }
            var bits = data.split("\n");
            IAM_ROLE = bits[0];
            outputLog("IAM-ROLE:", IAM_ROLE);
            found = true;
            fn();
        });
    });
    req.on('error', shitIAMError);
    req.end();
}

function shitIAMError(){
    errorLog('IAM-ERROR: Could not retrieve credentials. Bailing...');
    process.exit(1);
}

function getIAMCredentials(){
    var req = http.request({
        'host': '169.254.169.254',
        'path': '/latest/meta-data/iam/security-credentials/'+IAM_ROLE+'/'
    }, function(res){
        res.setEncoding('utf8');
        var data = "";
        res.on('data', function(d){ data += d; });
        res.on('end', function(){
        try {
            var j = JSON.parse(data);
            if(j.Code === "Success"){
                if(AWS_SECURITY_TOKEN !== j.Token){
                    //don't do anything if this hasn't changed.
                    AWS_ACCESS_KEY_ID = j.AccessKeyId;
                    AWS_SECRET_ACCESS_KEY = j.SecretAccessKey;
                    AWS_SECURITY_TOKEN = j.Token;
                    outputLog("IAM_UPDATE:", 'IAM Credentials Refreshed');
                    sqs.setCredentials(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY,AWS_SECURITY_TOKEN);
                    if(!RUNNING){
                        start();
                    }
                }
                //now trigger this to run again in a bit.
                setTimeout(function(){
                    getIAMCredentials();
                }, IAM_REFRESH_PERIOD);
            }else{
                shitIAMError();
            }
        }catch(e){
            shitIAMError();
        }
      });
    });
    req.on('error', shitIAMError);
    req.end(); //finish request.
}

var BUFFERS = {};

function messageToBuffer(l){
    var s = l.split("|"), t= s.shift(), q, d, m;
    if (isNaN(parseInt(t,10))) {
        q = t, d = 0, m = s.join("|");
    } else {
        d = t, q = s.shift(), m = s.join("|");
    }
    if(m){
        BUFFERS[q] = BUFFERS[q] || [];
        BUFFERS[q].push(d);
        BUFFERS[q].push(m);
        outputLog('BUFFER:', q, d, m);
    }
}

function flushBuffers(){
    for( var q in BUFFERS ){
        if(BUFFERS.hasOwnProperty(q) && BUFFERS[q].length > 0){
            dispatchQueue(q);
        }
    }
}

function dispatchQueue(q){
    var m, d;
    sqs.setQueue("/"+config.amazon_id+"/"+q+"/");
    while( BUFFERS[q].length > 0 ){
        d = BUFFERS[q].shift();
        m = BUFFERS[q].shift();
        sqs.request('SendMessage', {'MessageBody': m, 'DelaySeconds': d}, (function(q1,d1,m1){ return function(err,res){
            if(err){
                errorLog("ERROR:", err);
            }else{
                if(res.Error){
                    errorLog("ERROR:", res.Error.Message);
                }else{
                    outputLog("QUEUED:", q1, d1, m1, "SQS-ID:", res.SendMessageResult.MessageId);
                }
            }
        }; })(q,d,m));
    }
}

function LineBuffer(stream, encoding){
    if( encoding === undefined ){
        encoding = 'utf8';
    }
    var buffer = "";
    var emitter = new (require('events').EventEmitter)();
    stream.setEncoding(encoding);
    stream.on('data', function(data){
        if( !!~data.indexOf("\n") ){
            var parts = data.split("\n");
            parts[0] = buffer + parts[0];
            buffer = parts.pop();
            parts.forEach(function(l){
                emitter.emit('line', l);
            });
        }else{
            buffer += data;
        }
    });
    return {
        each: function(fn){
            emitter.on('line', fn);
        }
    };
};

function start(){
    var listener = tcp.createServer(function(conn){
      if(DEBUG){
        LineBuffer(conn).each(function(l){
            outputLog("DEBUG:", l);
            messageToBuffer(l);
        });
      }else{
        LineBuffer(conn).each(messageToBuffer);
      }
    });

    //dispatch queues every 0.1 seconds
    if(!DEBUG){
      setInterval(flushBuffers, 100);
    }
    //start listener
    listener.listen(config.port||7890, function(){
        outputLog('SQS Proxy Listening on '+config.port||7890);
        if(DEBUG){
            outputLog('Running in DEBUG Mode');
        }
    });
}

function outputLog(){
    var msg = logDate();
    [].slice.call(arguments,0).forEach(function(v){ msg += " "+v; });
    process.stdout.write(msg+"\n");
}

function errorLog(){
    var msg = logDate();
    [].slice.call(arguments,0).forEach(function(v){ msg += " "+v; });
    process.stderr.write(msg+"\n");
}

function pad(n){ return n>9 ? n : "0"+n; }

function padMS(n){
    return n>99 ? n : "0"+pad(n);
}

function logDate(){
    var d = new Date();
    return "["+
        [d.getUTCFullYear(),pad(d.getUTCMonth()+1),pad(d.getUTCDate())].join("-")+
        " "+
        [pad(d.getUTCHours()),pad(d.getUTCMinutes()),pad(d.getUTCSeconds())].join(":")+
        "."+
        padMS(d.getUTCMilliseconds())+
        "]";
}
