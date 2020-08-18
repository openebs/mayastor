# Summary

Nix tests for rebuild functionality

# Run the test

In order to run the test:
```
cd path/to/test
nix-build default.nix -A rebuild
```


 #  Rerun the tests

 Once the test has completed succesfully and you want to re-run it,
 the output has to be destroyed. Simple way to do this is:

 ```
 nix-store --delete ./result --ignore-liveness
 ```
