# GitLab CI/CD

We are currently using GitLab for CI/CD make using of auto scaling through Docker machine. Each
commit goes through:

1. Check style and run lint
2. Build binaries
3. Run tests
    - Run CSI tests
    - Run private API tests
    - Run rust tests for IO path

For the master branch there is optional 4th step:

4. Build docker images

Images are built and pushed only if explicitly requested (manual mode).
The reason why we don't push images for all master commits implicitly is
that we have three Docker images in the repo and we are not able to tell
which change applies to which image. Possible solution is to split the repo
(later when fundamentals don't change as much as they do now).

## Running CI tests locally

The cool thing about GitLab is that you run the tests locally. In order to do so
follow [this](https://docs.gitlab.com/runner/install/linux-manually.html) link to install
the runner. Once installed run the CI:

```bash
gitlab-runner exec docker compile
```

Note: that if you are not using debian as your host environment you might end up in with errors
like:

```bash
/code/target/debug/mayastor: error while loading shared libraries: libiscsi.so.8: cannot open shared object file: No such file or directory
```

A simple `cargo {clean,build --all}` will fix that.

## TODO

We would like to perhaps move, or add other CI pipelines:

 - Azure?
 - Circle CI
 - Travis
 - Github Actions