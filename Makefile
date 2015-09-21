test:
	ZK=$(shell docker-machine ip zk):2181 npm run test

.PHONY: test
