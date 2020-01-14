# GitLab CI/CD

We are currently using GitLab for CI/CD make using of auto scaling through
Docker machine. The master repository is hosted on
[github](https://github.com/openebs/MayaStor) and mirrored to
[gitlab](https://gitlab.com/awesome-mayastor/MayaStor-test) to run the CI/CD
pipeline there. Each commit goes through:

1. Check style and run lint
2. Build debug binaries
3. Run tests on debug binaries:
    - Run rust unit tests for IO path and module APIs
    - Run CSI RPC tests (mocha)
    - Run mayastor RPC tests (mocha)
    - Run moac unit tests (mocha)
4. Build docker images with production binaries using NIX

For the master branch there is optional last step:

5. Publish docker images to docker hub

Images are pushed only if explicitly requested (manual mode). For a complete
list of actions see [.gitlab-ci.yml](../.gitlab-ci.yml).

## Running CI tests locally

The cool thing about GitLab is that you run the tests locally. In order to do
so follow [this](https://docs.gitlab.com/runner/install/linux-manually.html)
link to install the runner. Once installed run the CI:

```bash
gitlab-runner exec docker compile
```

Note: that if you are not using debian as your host environment you might end
up in with errors like:

```bash
/code/target/debug/mayastor: error while loading shared libraries: libiscsi.so.8: cannot open shared object file: No such file or directory
```

A simple `cargo {clean,build --all}` will fix that.

## TODO

* We would like to add other CI pipelines deploying the images to k8s
  cluster and doing e2e tests.
* We would like to check coding style (and perhaps lint) using github
  actions instead of gitlab pipeline.
