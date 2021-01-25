# Mayastor and vscode editor

You will need to install a vscode [remote container plugin](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers).
Then you need to create a `.devcontainer.json` file in the top level directory
of mayastor repo. As a starting point you can use:

```json
{
    "image": "mayadata/ms-buildenv:latest",
    "workspaceMount": "source=${localWorkspaceFolder},target=/workspace,type=bind,consistency=delegated",
    "workspaceFolder": "/workspace",
    "mounts": [
        "source=rust-target-dir,target=/workspace/target,type=volume"
    ],
    "extensions": [
        "chenxsan.vscode-standardjs"
    ]
}
```

That tells vscode that it should start a `mayadata/ms-buildenv:latest` container
and run the vscode server inside of it. The directory with the sources will
be mounted at /workspace from the host.

## JavaScript

There are two components in mayastor project that are written in JS:

* moac
* test/grpc

For both of them the same vscode configuration applies. We will be using
[standardjs vscode plugin](https://marketplace.visualstudio.com/items?itemName=chenxsan.vscode-standardjs).
It is already named in the `.devcontainer.json` so it will be implicitly
installed into the container for you. Example of vscode user settings
used to configure the plugin:

```json
{
    "[javascript]": {
        "editor.defaultFormatter": "chenxsan.vscode-standardjs"
    },
    "standard.usePackageJson": true,
    "standard.autoFixOnSave": true,
    "standard.semistandard": true,
    "standard.workingDirectories": [
        "csi/moac",
        "test/grpc"
    ]
}
```

Now if you open an arbitrary JS file in the editor, you should see all errors
reported by semistandard marked in red. When you save a file, the plugin will
try to autofix all problems that it sees in the file. Happy hacking!

## Rust

TODO
