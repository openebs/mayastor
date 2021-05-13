# Purpose
To test things in containers but on the same host, we have the composer library in Rust.
This works pretty well but has the downside that if you want to add something to
that test, you have to recompile the tests all the time, and you can not leave the
containers running when the test fails without modifying the tests either. This is
because those tests are part of our CI/CD pipeline, so we must clean them up.

Additionally, when working on the data plane and you want to test, let us say,
"child retire" logic. It can be rather a pain to do that manually, even when
scripted. To fill this cap, we can use pytest, which can a far richer than bash
scripts - and it can leverage docker-compose.  Upside is that the test suites
can run with -- or without starting the containers.

So all in all, the purpose is to create reproducible tests/environments.

For example, I can start my containers and run:
```
docker-compose up
pytest nexus.py --docker-compose-no-build --use-running-containers
```
This allows me to see the logs in real-time, and on failure, figure out why it
failed and so forth.

Without the extra arguments however, the test suite will create and destroy
the containers

# Setup

Depending on the used OS/distro of choice you will need to install some
python packages. To make it easier. You can also use virtual environments.

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install python packages:


```bash
pip install -r requirements.txt
```

# Protobuf

`shell.nix` contains a few python3 packages in particular for proto generation.
From within the `rpc/proto` run:

```bash
python -m grpc_tools.protoc -I . --proto_path=. --python_out=. --grpc_python_out=. mayastor.proto
```

And copy them (yes -- on the TODO list) over when they where updated, such that:

```bash
docker-compose.yml  mayastor_pb2_grpc.py  mayastor_pb2.py  nexus.py  README.md  requirements.txt
```

# TODO:
 [ ] fix proto generation
 [ ] create test suite layout such that the tests fixtures can be re-used
 [ ] create test that run fio, do "create 110" volumes etc type like tests
 [ ] ...
