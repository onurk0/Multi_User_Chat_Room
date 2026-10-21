# Multi-User Chat Room

A multi-room, multi-client terminal chat application written in C using POSIX sockets and `pthreads`. A central server accepts TCP connections, groups clients into rooms, and relays messages between them. Users can also send files to each other within a room.

![Language](https://img.shields.io/badge/language-C-blue)
![Platform](https://img.shields.io/badge/platform-Linux-lightgrey)

> **Platform note:** This project targets **Linux**. On Windows, use WSL. It may build on other POSIX systems but is only tested on Linux.

## Screenshot

<!-- Replace this comment with a screenshot or GIF of a chat session, e.g.:
![Chat session](docs/screenshot.png)
-->

---

## Table of Contents

- [Features](#features)
- [How It Works](#how-it-works)
- [Requirements](#requirements)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [File Transfer](#file-transfer)
- [Project Structure](#project-structure)
- [Limitations](#limitations)
- [Troubleshooting](#troubleshooting)
- [Future Improvements](#future-improvements)

---

## Features

- **Multiple chat rooms**: create a new room or join an existing one, with live occupant counts shown when you connect
- **Concurrent clients**: the server spawns one thread per client
- **Colored usernames**: each user's messages are displayed in a color derived from their username
- **Join/leave notifications**: everyone in a room is notified when someone enters or exits
- **Server-side logging**: the server prints new connections and a list of connected clients (name, IP, room)
- **Peer file transfer**: send a file to another user in your room, with an accept/decline prompt, progress display on both sides, and cleanup of partial files if a transfer fails

## How It Works

```
 ┌──────────┐                    ┌──────────────────┐                    ┌──────────┐
 │ Client A │ ◄── TCP :1004 ───► │      Server      │ ◄── TCP :1004 ───► │ Client B │
 └──────────┘                    │  (thread/client) │                    └──────────┘
                                 │  room tracking   │
 ┌──────────┐                    │  message relay   │
 │ Client C │ ◄── TCP :1004 ───► │  file relay      │
 └──────────┘                    └──────────────────┘
```

- The server keeps a linked list of connected users (protected by a mutex), each tagged with a room ID.
- Chat messages are broadcast only to other users in the sender's room.
- Every message on the wire is a newline-terminated text line. File data is sent as length-prefixed chunks (`[FILE_CHUNK] <n>` followed by exactly `n` raw bytes), so chat can keep flowing to a user while they receive a file without corrupting it.
- File transfers are **relayed through the server**: the sender streams chunks to the server, and the server forwards them to the receiver. A per-socket write lock keeps messages from different threads from interleaving.
- Each client runs two threads: one for reading keyboard input and one for receiving data from the server.

The full wire protocol is documented at the top of [main_server.c](main_server.c).

## Requirements

- A Linux environment
- `gcc` (or edit `CC` in the Makefile to use `clang`)
- `make`
- POSIX threads (`pthread`), included with glibc

## Getting Started

### 1. Clone and build

```bash
git clone https://github.com/onurk0/Multi_User_Chat_Room.git
cd Multi_User_Chat_Room
make
```

This produces two executables: `main_server` and `main_client`.

### 2. Start the server

By default the server listens on **port 1004**. Because this is a privileged port (below 1024), the server must be run with `sudo`:

```bash
sudo ./main_server
```

You should see:

```
[SERVER] File transfer enabled chat server started on port 1004
```

To avoid `sudo`, build both programs with an unprivileged port instead (see [Changing the port](#changing-the-port)).

> Always start the server **before** any clients.

### 3. Connect a client

In a separate terminal (or on a different machine on the same network):

```bash
./main_client <server-ip>
```

If the server is running on the same machine:

```bash
./main_client 127.0.0.1
```

To find the server's IP address, run this on the server machine:

```bash
ip addr
```

### 4. Clean up

```bash
make clean
```

### Changing the port

The port is set at compile time by the `PORT_NUM` macro in [net_common.h](net_common.h). Override it when building (the server and client must use the same port):

```bash
make clean
make CFLAGS="-Wall -Wextra -O2 -DPORT_NUM=5000"
./main_server              # no sudo needed for ports >= 1024
```

## Usage

### Joining a room

| Situation | What happens |
|---|---|
| You are the first client to connect | A new room is created automatically, then you're prompted for a username |
| Rooms already exist | You see a list of rooms with occupant counts, then choose a room number or type `new` to create one |

You can also skip the interactive prompt by passing a room argument:

```bash
./main_client <server-ip> new      # create a new room
./main_client <server-ip> 2        # join room 2
```

You have 30 seconds to pick a room and enter a username before the server drops the connection.

### Chatting

Type a message and press **Enter**. Everyone else in your room will see it, prefixed with your name and IP address. Press **Ctrl+C** or **Ctrl+D** to exit.

Lines starting with `[FILE_` are reserved for the protocol and won't be sent as chat.

## File Transfer

To send a file to another user in your room:

```
SEND <username> <path>
```

Example:

```
SEND alice notes.txt
SEND alice ../docs/report.pdf
```

**What happens:**

1. The recipient sees a prompt: `Accept? (Y/N):` and answers on their next line of input.
2. The sender waits up to **60 seconds** for an answer; if none arrives, the request is cancelled.
3. On **Y**, the file is streamed in 4 KB chunks and both sides display a progress percentage.
4. The file is saved in the recipient's current directory as `received_<filename>`. Any directory part of the name is stripped, so a sender can't write outside that directory.
5. If the transfer fails partway (either side disconnects, or the sizes don't match), the partial file is deleted.

Other users in the room can keep chatting while a transfer is in progress, and the recipient keeps receiving chat. The sender's input is busy until their transfer finishes.

## Project Structure

```
.
├── main_server.c   # Server: connections, rooms, message broadcast, file relay, protocol docs
├── main_client.c   # Client: room handshake, send/receive threads, colors, file transfer
├── net_common.h    # Shared constants (port, buffer sizes) and buffered socket I/O helpers
├── Makefile        # Builds both executables (make / make clean)
└── README.md
```

## Limitations

**File transfer**
- Filenames and usernames used with `SEND` **cannot contain spaces** (the command is split on spaces).
- A user can send one file and receive one file at a time; a second request gets a "busy" error.
- Transfers are relayed through the server, so large files use server bandwidth and are limited by it.
- Received files are always written as `received_<filename>` in the current directory, overwriting any existing file with that name.

**General**
- **Linux only.** Not tested on macOS or Windows (except via WSL).
- **Compile-time port.** The port can only be changed by rebuilding (see [Changing the port](#changing-the-port)).
- **IPv4 only.** The client takes an IPv4 address, not a hostname.
- **No encryption or authentication.** Messages and files are sent in plaintext over TCP, and anyone who can reach the server can join. Use only on trusted networks.
- **Unique usernames are not enforced.** If two users in a room share a name, `SEND <username>` goes to whichever joined first.
- **Message size.** Lines are limited to about 510 characters (longer input is split into several messages); usernames are truncated to 49 characters.
- **Room listing is capped.** Only the first 20 rooms are shown when joining.
- **Rooms are temporary.** A room exists only while it has users; room IDs are not reused, and nothing is persisted (no chat history).
- **Colors.** Usernames are hashed onto 6 ANSI colors, so different users can share a color. Your terminal must support ANSI escape codes.
- **Scale.** The server uses one thread per client, accepts sockets numbered below 1024, and each broadcast reaches at most 100 recipients. It's meant for small groups, not large deployments.

## Troubleshooting

| Problem | Likely cause / fix |
|---|---|
| `ERROR on binding: Permission denied` | Port 1004 is privileged. Run the server with `sudo`, or rebuild with a port ≥ 1024 |
| `ERROR on binding: Address already in use` | Another server instance is already running on that port |
| `ERROR connecting` | Server isn't running, wrong IP, port mismatch between builds, or a firewall is blocking the port |
| `Invalid IP address` | The client needs a numeric IPv4 address such as `127.0.0.1`, not a hostname |
| `ERROR: Room N does not exist` | Rooms only exist while someone is in them. Start with `new` or use the room list |
| `ERROR: server closed the connection` while joining | You took longer than 30 seconds to choose a room or enter a username |
| Garbled characters instead of colors | Your terminal doesn't support ANSI escape codes |

## Future Improvements

- Configurable port via command-line argument
- Unique-username enforcement
- Support for spaces in filenames and usernames in `SEND`
- Choice of download location for received files
- TLS encryption
- Hostname/IPv6 support

---

*Built as a networking and systems programming project using C, POSIX sockets, and pthreads.*
