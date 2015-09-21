#### ZK-Orca

[![Build Status](https://travis-ci.org/racker/zk-orca.svg)](https://travis-ci.org/racker/zk-orca)

An orchestration library.

## Testing

Create the docker machine:

```
docker-machine create zk
eval $(docker-machine env zk)
```

Start the zookeeper instance:

```
docker-compose start zookeeper
```

Run tests:

```
make test
```
