ToyMapReduce-go
===============

**toymapreduce-go** is an implementation of mapreduce.

**some test cases and test code are copy from git://g.csail.mit.edu/6.824-golabs-2022**

Features
--------

- rpc coordinator server which matains workers status and schedule tasks to workers

- fault tolerance for worker failure

Play
----

build from source code and run test case.

```shell
git clone https://github.com/sunyakun/toymapreduce-go
cd toymapreduce-go
make build

# run test case
./play.sh
```