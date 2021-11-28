// Author: Eric Whalls
// Client Example for connection to IO Cube 

// Uses TCP command to create scanner 
// Uses UDP to ingest all incoming data 
// TCP command returns the headers for the data format 


// Protocols:
const { networkInterfaces } = require('os');

// Defines:
var headers = [] // received from module

// Conversions: 
function bin2String(array) {
    return String.fromCharCode.apply(String, array);
}

//--------------------

// UDP
const dgram = require("dgram");
const UDP = dgram.createSocket('udp4')

UDP.on('message', function(msg) {
    var decoded = bin2String(msg);
    var values = decoded.split(",")
    if(headers.length === values.length) {
        for(let i=0; i < headers.length; i++) {
            console.log(headers[i]+"\t"+values[i]);
        }
    }
    console.log("\n")
})

UDP.bind(5111);

// TCP
const stream = require('net')
const TCP = new stream.Socket(); 

TCP.connect(5550, 'localhost', function() {
    TCP.write('scan 5111\r\n')
})

TCP.on('data', function(data) {
    data = bin2String(data)
    headers = data.split(',')
})