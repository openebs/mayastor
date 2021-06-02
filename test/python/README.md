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
the containers automatically. This is done by making use of the pytest fixtures

# Setup

`nix-shell` and have fun.

# TODO:
 [ ] fix proto generation
 [ ] create test suite layout such that the tests fixtures can be re-used
 [ ] create test that run fio, do "create 110" volumes etc type like tests
 [ ] ...
