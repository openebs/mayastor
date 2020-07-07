## AWS Linux

EKS uses this image as the base operating system. It is based on Redhat Enterprise
Linux 7. During testing, it might, sometimes, be useful to be able to create such
an image locally and see what breaks.

We are not installing k8s however -- as its unclear what type of binaries we
would need from AWS as it is very likely they are not the binaries from k8s
upstream (which we typically use).

As of today, the kernel used is 4.14, which is rather old so there will be no
NVMe, nor will there be any io_uring support.

## Usage

Make sure you edit main.tf to set the proper variables like user keys, image
etc.

By default, a user "ec2-user" is available on the ISO, the password of this user
is set within the meta-data file. Additionally, a new user is created that
points to your local user and pick up the right SSH keys from there.

Also, i'd recommend to download the QCOW image locally as by default it will be
fetched from the internet.
