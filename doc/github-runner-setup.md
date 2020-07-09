enable nested virtualisation inthe hypervisor

Edit so that:
$ cat ~/.config/nix/nix.conf
system-features = kvm nixos-test

sudo usermod -aG kvm $(whoami)

sudo reboot

sudo modprobe kvm_intel nested=1 # TOMTODO Find a way to make persistent
