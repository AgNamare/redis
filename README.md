Mini Redis Server in Python

This is a small in-memory key-value store written in Python. It supports basic Redis-like commands, key expiration, and saving/loading data from disk.

Features

Basic commands: SET, GET, DEL, EXISTS, INCR, DECR

List commands: LPUSH, RPUSH

Utility commands: PING, ECHO, SAVE

Key expiration with EX, PX, EXAT, PXAT

Persistent storage to db.json

Handles multiple clients at the same time using threads

Getting Started

Clone the repo:

git clone https://github.com/username/redis.git
cd redis

Install dependencies:

pip install resp -> contains the serializer and deserializer of the RESP protocol of redis

Run the server:

python main.py

Usage

Connect using a TCP client or you can just use the actual redis-cli.

Examples
SET mykey "hello"
GET mykey
SET counter 10 EX 60
INCR counter
DECR counter
LPUSH mylist 1 2 3
RPUSH mylist 4 5
EXISTS mykey counter
DEL mykey counter
SAVE
