# Multi User Chat Room

This is a multi user chat room that utilizes network and socket programming
in C using Linux libraries. 

**_Note: This program may or may not work on MacOS-based systems and Windows.
This is intended to be run on Linux based environments_**

To run the program:
1. Download a C compiler, like `GCC` or `clang`
2. Download the source code and extract it to a directory
3. Enter the direcory containing the source code
- First, run the server before starting any clients
- To run, type `make` then `./main_server`
4. To join a room, type `./main_client **IP address of server**`
- Example Usage: `./main_client 127.0.0.1` (Running server on same machine as 
client)
- To find the IP address of the device running the server, type `ip addr`
5. If a client is the first to join, it will automatically be given a new room
and then be prompted for their name
- Subsequent clients will be asked to choose a room first, then enter their name
6. Once connected, each client will be assigned a random text color
7. Clients can transfer file to each other by doing the following:
- `SEND _receiving user name_ _file to be transferred`
- The receiving client will asked if they want to accept or decline the transfer
(Y/N). The file will be stored in the client's current directory.
- When Y is entered, the transmission will begin. During file transfers, other
clients who are chatting will not be impacted during the duration of the
transfer.

To remove all executable files, type `main clean`

