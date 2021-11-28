import os,sys 
import threading
from multiprocessing import Event
import socket
import time 
import json

from Parameters import Parameters
Parameters = Parameters.instance()

"""
    Author: Eric Whalls

    tell the program to bombard you with udp packets 
    how to manual:

    telnet [host] 5110 
    > scan [your-port]
    > replies with all the keys fromt he configuration 

    See screen shots in shared drive for info under Design > Simulator 

"""

class JobHandler(threading.Thread) : 

    class TCPJob(threading.Thread) :
        
        def __init__(self, connection, client, keys) :
            threading.Thread.__init__(self)
            ##
            self.con = connection
            self.client = client 
            self.shutdown = Event()

            self.parameters = [] 
            # grab the memory locations of the parameters we want to scan 
            for key in keys :
                parameter = getattr(Parameters, key) 
                self.parameters.append(parameter)
            #
            self.start() 

        def run(self) :
            while not self.shutdown.is_set() : 
                time.sleep(0.1) 
                #
                values = [str(par.value) for par in self.parameters]
                data_string = ",".join(values)
                data_bytes = (data_string+"\r\n").encode('ascii')
                #
                try :
                    self.con.sendall(data_bytes)
                except Exception as e :
                    self.shutdown.set() 
                    print("TCP JOB Exception", e)

    class UDPJob(threading.Thread) :
        def __init__(self, remote_host, remote_port, keys) :
            threading.Thread.__init__(self)
            self.shutdown = Event()
            #
            self.address = (remote_host[0], remote_port)
            #
            self.parameters = [] 
            # grab the memory locations of the parameters we want to scan 
            for key in keys :
                parameter = getattr(Parameters, key) 
                self.parameters.append(parameter)
        
            # create udp socket stream
            self.sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

            # start sending
            self.start()

        def run(self) :
            while not self.shutdown.is_set() :
                time.sleep(0.1) # 10hz
                #
                values = [str(par.value) for par in self.parameters]
                data_string = ",".join(values)
                data_bytes = (data_string+"\r\n").encode('ascii')
                try :
                    self.sock.sendto(data_bytes, self.address)
                except Exception as e :
                    self.shutdown.set()
                    print("UDP job terminated due to error\n", e)
            
            self.sock.close()
            del self.sock 

    def __init__(self, port=5550) :
        threading.Thread.__init__(self)
        self.shutdown = Event()
        #
        self.address = ('localhost', port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(self.address)
        self.sock.listen(5)
        #
        self.jobs = []
        #
        self.start() 

    def stop(self) :
        self.shutdown.set() 
        for job in self.jobs :
            job.shutdown.set() 

    def getAllKeys(self) : 
        parameters = Parameters.get()
        keys = []
        for p in parameters : 
            par = getattr(Parameters, p)
            if hasattr(par, "toList") : 
                keys.append(p)
        #
        return keys

    def parseHTTP(self, req) :
        #
        fields = req.split("\r\n")
        if "GET" in fields[0] : 
            fields = fields[1:] #ignore the GET / HTTP/1.1
            output = {}
            for field in fields:
                if not field:
                    continue
                key,value = field.split(':')
                output[key] = value
            #
            return output 
        ##
        return None


    def run(self) : 
        print("TCP command server started on localhost:"+str(self.address[1]))
        while not self.shutdown.is_set() :
            time.sleep(1)
            # allow connections 
            try : 
                con, client = self.sock.accept() 
                data = con.recv(1024)
                data = data.decode('ASCII')
                #
                # returns none if no http GET is detected
                print(data)
                args = data.split(" ")
                # messy command interpreter (for UDP only)
                if "GET" in data :
                    # http request:
                    keys = self.getAllKeys()
                    self.jobs.append(self.TCPJob(con, client, keys))
                elif len(args) >= 1 : 
                    if args[0] == "scan" : 
                        port = int(args[1])
                        keys = self.getAllKeys()
                        # for now just send all keys in system 
                        self.jobs.append(self.UDPJob(client, port, keys))
                        # respond with keys
                        con.sendall((",".join([str(k) for k in keys])+"\n\r").encode("ascii"))
                        con.close()
                else :
                    con.sendall("Error, command not found\n\r".encode("ascii"))
                    con.close() 
                # close the connection  
                #con.close() 
        
            except Exception as e : 
                print("Error occured:\n",e)
            




