import sys
import time
import asyncio
import aiohttp
import json
import argparse # So that I don't need to write stuff dealing with argv
import logging # As suggested in Discussion 1B's slides

# Flags
log = True

# Consts
API_KEY = '' # Google API Key

#server info as dicts
servers = {"Hill": 12205, "Jaquez": 12206, "Smith": 12207, "Campbell": 12208, "Singleton": 12209}
server_connection = {
    "Hill": ["Jaquez", "Smith"],
    "Jaquez": ["Hill", "Singleton"],
    "Smith": ["Hill", "Singleton", "Campbell"],
    "Campbell": ["Smith", "Singleton"],
    "Singleton": ["Jaquez", "Smith", "Campbell"]
}
# Graphically, looks like the following:

    #           Singleton
    #          /     |   \
    #         /      |    \
    #        /       |     \
    #    Jaquez      |    Campbell
    #       |        |      /
    #       |        |     /
    #       |        |    /
    #       |        |   /
    #     Hill ---- Smith


# tcp connection code adapted from the python documentation examples https://docs.python.org/3/library/asyncio-stream.html#asyncio-streams


# Server Code adapted from TA Code Help Repository echo_server.py https://github.com/CS131-TA-team/UCLA_CS131_CodeHelp/blob/master/Python/echo_server.py
    
def write_to_log(log_text):
    if(log):
        logging.info(log_text)
#end write_to_log

def is_number(string):
    try:
        float(string)
        return True
    except ValueError:
        return False

def check_IAMAT(split_msg):
    temp = split_msg[2].replace('+', '-') # Unify the delimeter
    nums = list(filter(None, temp.split('-')))
    if len(nums) != 2 or not (is_number(nums[0]) and is_number(nums[1])):
        return False
    # check 4th argument float
    if not is_number(split_msg[3]):
        return False
    return True

def is_between (a, lower_bound, upper_bound):
    return ((a>=lower_bound) and (a <= upper_bound))


def handle_invalid_msg(split_msg):
    return_msg = "?"+" ".join(split_msg) # this recreates the original message
    return  return_msg
#end handle_invalid_msg

def get_timediff(split_msg):
    cur_time = time.time()
    msg_timestamp = float(split_msg[3])
    time_diff = cur_time - msg_timestamp
    return time_diff
# end get_timediff

class Server:
    def __init__(self, name, ip, port):
        self.name = name # Hill/Jaquez/Singleton/Campbell/Smith
        self.ip = ip 
        self.port = port # Corresponding ports 12205-12209
        self.clients = {}
        self.client_messages = {}

    def check_WHATSAT(self, split_msg):
    # check if current client exists
        if (split_msg[1] not in self.clients):
            return False

        # verify radius and bound are numbers
        if (not (is_number(split_msg[2]) and is_number(split_msg[3]))):
            return False
        # now radius and bound are numbers
        rad = int(split_msg[2])
        bound = int(split_msg[3])
        # check ranges
        if(is_between(rad, 0, 50) and is_between(bound, 0, 20)):
            return True
        else:
            return False
    # end check WHATSAT


    async def broadcast(self, msg):
        for reachable in server_connection[self.name]:
            try:
                reader, writer = await asyncio.open_connection('127.0.0.1', servers[reachable])
                writer.write(msg.encode())
                await writer.drain()
                write_to_log(f"{self.name} Broadcasted to {reachable}: {msg}")
                write_to_log(f"{self.name} terminating transmittion to {reachable}")
                writer.close()
                await writer.wait_closed()
            except:
                write_to_log(f"{self.name} Failed to establish connection with {reachable}")
        #end for
    #end broadcast

    def get_coords(self, location):
        plus = location.rfind('+')
        minus = location.rfind('-')
        if plus != -1 and plus != 0:
            return f"{location[0:plus]},{location[plus:]}"
        if minus != -1 and minus != 0:
            return f"{location[0:minus]},{location[minus:]}"
        return None

    # Code adapted from TA Code Help Repository, aiohttp_example.py
    async def query(self, session, url):
        async with session.get(url) as response:
            return await response.text()

    # Code adapted from TA Code Help Repository, aiohttp_example.py
    async def search_places(self, location, radius, bound):
        async with aiohttp.ClientSession() as session:
            coords = self.get_coords(location)
            if (coords!= None):
                write_to_log(f"Searching at coordinates {coords}, Radius = {radius}.")
                url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?key={0}&location={1}&radius={2}'.format(API_KEY, coords, radius)
                search_result = await self.query(session, url)
                decoded_result = json.loads(search_result)
                num_places = len(decoded_result["results"])
                write_to_log(f"Obtained {num_places} place(s) from Google API")
                if len(decoded_result["results"]) <= int(bound):
                    return search_result
                else:
                    # need to filter out extra results from result object
                    decoded_result["results"] = decoded_result["results"][0:int(bound)]
                    return json.dumps(decoded_result, sort_keys=True, indent=4)
    #end search places


    async def handle_split_msg(self, split_msg):
        #print(f"{self.name} handling split messages")
        argnum = len(split_msg)
        sendback_message = "DEFAULT SENDBACK MESSAGE" # this will be returned
        if(argnum == 4): # IAMAT and WHATSAT requests
            command = split_msg[0]
            if (command == "IAMAT"):
                # handle IAMAT
                if(check_IAMAT(split_msg)):
                    # get timestamp difference
                    tdiff = get_timediff(split_msg)
                    if (tdiff >= 0):
                        diff = "+" + str(tdiff) 
                    else:
                        diff = str(tdiff) # since we already have a - before the number
                    # prepare sendback msg
                    sendback_message = f"AT {self.name} {diff} {split_msg[1]} {split_msg[2]} {split_msg[3]}"
                    await self.broadcast(sendback_message)
                else:
                    # error message
                    sendback_message = handle_invalid_msg(split_msg)
                
            #end IAMAT

            elif (command == "WHATSAT"):
                # handle WHATSAT
                if(self.check_WHATSAT(split_msg)):
                    location = self.client_messages[split_msg[1]].split()[4]
                    radius = split_msg[2]
                    bound = split_msg[3]
                    places = await self.search_places(location, radius, bound)
                    return_places = str(places).rstrip('\n') #remove the ending empty lines
                    sendback_message = f"{self.client_messages[split_msg[1]]}\n {return_places} \n \n"
                else:
                    sendback_message = handle_invalid_msg(split_msg)

            #end WHATSAT
            else: # This means the command is illegal
                sendback_message = handle_invalid_msg(split_msg)

        elif (argnum == 6):
            if(split_msg[0] == "AT"):
                # message broadcasted from other servers
                write_to_log("Recieved broadcast")
                sendback_message = None
                client_ID = split_msg[3]
                msg_time = float(split_msg[5])
                msg = " ".join(split_msg)
                if (client_ID in self.clients):
                    # Returning client
                    write_to_log(f"Already recieved message from Client: {client_ID}")
                    if(msg_time > self.clients[client_ID]): #if message is more up to date
                        self.clients[client_ID] = msg_time 
                        self.client_messages[client_ID] = msg
                        await self.broadcast(msg)
                    else:
                        write_to_log("Recived duplicate broadcast. Terminating broadcast.")
                else:
                    # New client
                    write_to_log(f"Server {self.name} recieved new client: {client_ID}")
                    self.clients[client_ID] = msg_time 
                    self.client_messages[client_ID] = msg
                    await self.broadcast(msg)

            else: #illegal command
                sendback_message = handle_invalid_msg(split_msg)
            #end AT

        else: #illegal command
            sendback_message = handle_invalid_msg(split_msg)
        
        return sendback_message 
    #end handle_split_msg

    async def handle_echo(self, reader, writer):
        #print(f"{self.name} handling echo now\n")
        #reading message
        while not reader.at_eof(): #while still have things to read
            # read and decode
            data = await reader.readline()
            message = data.decode()
            # log this action
            write_to_log(f"{self.name} recieved message: {message}") 
            split_msg = message.split()
            sendback_message = await self.handle_split_msg(split_msg) # this deals with different cases of input message, and generates the return message.

            write_to_log(f"{self.name} send: {sendback_message}")
            # write message back to client
            if (sendback_message != None):
                writer.write(sendback_message.encode())
                await writer.drain()

        write_to_log("close the client socket")
        writer.close()

    #end handle_echo


    async def run_forever(self):
        write_to_log(f"Booting Server: {self.name}")
        server = await asyncio.start_server(self.handle_echo, self.ip, self.port)

        # Serve requests until Ctrl+C is pressed
        print(f'serving on {server.sockets[0].getsockname()}')
        async with server:
            await server.serve_forever()
        # Close the server
        write_to_log(f"Shutting down Server: {self.name}")
        server.close()
    #end run_forever

#end class Server

# Some code adapted from TA Code Help Repository, echo_server.py
def main():
    #print("Main called")
    parser = argparse.ArgumentParser('CS131 Project argument parser')
    parser.add_argument('server_name', type=str, help='Server Names')
    args = parser.parse_args()
    if not args.server_name in servers:
        print(f"Invalid Server: {args.server_name}")
        sys.exit()
    #Initiate logging
    logging.basicConfig(filename="server_{}.log".format(args.server_name), format='%(levelname)s: %(message)s', filemode='w+', level=logging.INFO)

    #Start up server
    print(f"Starting up server: {args.server_name}\n")
    server = Server(args.server_name, '127.0.0.1', servers[args.server_name])
    try:
        asyncio.run(server.run_forever())
    except KeyboardInterrupt:
        print(f"Keyboard Interrupt. Shutting down server: {args.server_name}\n")
        pass

#end main


if __name__ == '__main__':
    main()
