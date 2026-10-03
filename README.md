# Multi-User Chat Room

A multi-room, multi-client terminal chat application written in C using POSIX sockets and `pthreads`. A central server accepts TCP connections, groups clients into rooms, and relays messages between them. Clients can also request direct file transfers with other users in the same room.

![Language](https://img.shields.io/badge/language-C-blue)
![Platform](https://img.shields.io/badge/platform-Linux-lightgrey)
![Status](https://img.shields.io/badge/file%20transfer-experimental-orange)

> **Platform note:** This project targets **Linux**. It uses glibc headers (e.g. `<bits/pthreadtypes.h>`) and may not compile on macOS or Windows without modification. On Windows, use WSL.

<!-- Add a screenshot or GIF of a chat session here:
![Demo](docs/demo.gif)
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

---

## Features

- **Multiple chat rooms**: create a new room or join an existing one, with live occupant counts shown when you connect
- **Concurrent clients**: the server spawns one thread per client
- **Colored usernames**: each user's messages are displayed in a color derived from their username
- **Join/leave notifications**: everyone in a room is notified when someone enters or exits
- **Server-side logging**: the server prints connections and a list of connected clients (name, IP, room)
- **Peer file transfer** *(experimental)*: send a file to another user in your room, with an accept/decline prompt on the receiving end

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
- File transfers are **relayed through the server**: the sender streams bytes to the server, and the server forwards them to the receiver.
- Each client runs two threads: one for reading keyboard input and one for receiving data from the server.

## Requirements

- A Linux environment
- A C compiler (`gcc` or `clang`)
- `make`
- POSIX threads (`pthread`), included with glibc

## Getting Started

### 1. Clone and build

```bash
git clone https://github.com/<your-username>/<repo-name>.git
cd <repo-name>
make
```

This produces two executables: `main_server` and `main_client`.

### 2. Start the server

The server listens on **port 1004**. Because this is a privileged port (below 1024), the server must be run with `sudo`:

```bash
sudo ./main_server
```

You should see:

```
[SERVER] File transfer enabled chat server started on port 1004
```

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

### Chatting

Type a message and press **Enter**. Everyone else in your room will see it, prefixed with your name and IP address. Press **Ctrl+C** to exit.

## File Transfer

> ⚠️ **Experimental.** File transfer works, but not reliably in every case. See [Limitations](#limitations).

To send a file to another user in your room:

```
SEND <username> <file>
```

Example:

```
SEND alice notes.txt
```

**What happens:**

1. You send the request, and the sender waits for a response.
2. The recipient sees a prompt: `Accept? (Y/N):`
3. On **Y**, the transfer begins and both sides display a progress percentage.
4. The file is saved in the recipient's current directory as `received_<filename>`.

Other users in the room can keep chatting while a transfer is in progress.

## Project Structure

```
.
├── main_server.c   # Server: connections, rooms, message broadcast, file relay
├── main_client.c   # Client: UI, send/receive threads, colors, file transfer
├── Makefile        # Builds both executables (make / make clean)
└── README.md
```

## Limitations

**File transfer (known issue)**
- File transfer is functional but **not fully reliable**, and should be treated as a proof of concept.
- Filenames and usernames **cannot contain spaces** (the `SEND` command is split on spaces).
- The server aborts a transfer if the receiver does not become ready within **10 seconds**.
- Transfers are relayed through the server, so large files use server bandwidth and are limited by it.
- Received files are always written as `received_<filename>`; there is no choice of destination folder.

**General**
- **Linux only.** Not tested or supported on macOS or Windows (except via WSL).
- **Fixed port.** Port `1004` is hard-coded in both `main_server.c` and `main_client.c`. Running the server requires `sudo` because it is a privileged port. To change it, edit `PORT_NUM` in both files and rebuild.
- **IPv4 only.** The client takes an IPv4 address, not a hostname.
- **No encryption or authentication.** Messages and files are sent in plaintext over TCP, and anyone who can reach the server can join. Use only on trusted networks.
- **Unique usernames are not enforced.** Two users with the same name in one room can make `SEND <username>` ambiguous.
- **Message size.** Messages are limited to roughly 255 characters; usernames to 49.
- **Room listing is capped.** Only the first 20 rooms are shown when joining.
- **Rooms are temporary.** A room exists only while it has users; room IDs are not reused, and nothing is persisted (no chat history).
- **Color assignment.** Colors come from 6 ANSI colors and are only tracked for the first 10 distinct users a client sees; additional users all display in red. Your terminal must support ANSI escape codes.
- **Scale.** The server uses one thread per client and a fixed broadcast buffer of 100 recipients. It's meant for small groups, not large deployments.

## Troubleshooting

| Problem | Likely cause / fix |
|---|---|
| `ERROR on binding` | Server wasn't run with `sudo`, or port 1004 is already in use (an old server instance may still be running) |
| `ERROR connecting` | Server isn't running, wrong IP, or a firewall is blocking port 1004 |
| `ERROR: Room N does not exist` | Rooms only exist while someone is in them. Start with `new` or use the room list |
| Build errors on macOS | Expected. The code relies on Linux/glibc headers |
| Garbled characters instead of colors | Your terminal doesn't support ANSI escape codes |

## Future Improvements

- Fix and harden file transfers (spaces in filenames, partial reads, timeouts, error recovery)
- Configurable port via command-line argument
- Unique-username enforcement
- TLS encryption
- Hostname/IPv6 support

---

*Built as a networking and systems programming project using C, POSIX sockets, and pthreads.*
