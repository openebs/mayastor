
When making changes (or debugging) SPDK it is far more convenient to have a
local checkout of SPDK rather than dealing with packages. To make this a bit
easier, it is possible to enter the development environment by passing an
optional argument.

```
nix-shell --arg nospdk true
```

The above will result in a shell, where there is no SPDK. In order to develop an
environment like this, it is assumed that you will have a local checkout of SPDK
within the spdk-sys directory.

```
cd ${workspace}/spdk-sys
git clone https://github.com/openebs/spdk
cd spdk
git checkout mayastor-x.y
git submodule update --init --recursive
cd ..
./build.sh
```

The above (when the proper values for x and y are satisfied) will result in a
libspdk.so within the spdk directory. When building, the build script will pick
up the library.

Note that when you want to switch back, you have to ensure, that the spdk dir is
removed (or renamed) to avoid it including or linking by accident.



