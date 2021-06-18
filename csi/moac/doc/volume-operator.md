# Volume Operator

Volume operator is responsible for volume creation, destruction and recovery of
volumes on storage nodes.  By volume, we mean "persistent volume" in K8s, which
is dynamically created as a result of creating "persistent volume claim". A
volume consists of a nexus and replica(s). Nexus is a logical volume, entry
point for IOs, which are further dispatched to replicas - physical volumes,
which allocate space from storage pools and can be accessed either locally (in
the same process where nexus lives) or using remote storage protocol NVMEoF.

The volume operator relies on following information when making decisions:

* info from CSI requests (i.e. create volume parameters from CSI RPC method)
* info obtained from storage nodes (i.e. list of created replicas and their parameters)
* info obtained from msv k8s custom resources (i.e. was the volume mounted)

When moac starts up it reads all available information from storage nodes and
then tries to create/modify objects according to what it believes is the desired
state. The source of the information about the desired state are msv resources.
For example, if it finds a nexus with three replicas but only two replicas exist,
it will create the third one. If it finds three replicas belonging to the same
volume and msv says that the volume is used, it will create the missing nexus
and connect it to the replicas.

## Mayastor Volume Custom Resource

The "msv" resource is created when the volume is created (using CSI) and
destroyed when the volume is destroyed using CSI or the custom resource is
deleted by kubectl. Users should not create msv resources manually bypassing
CSI.  Except the replica count parameter, msv resource should not be updated by
user.  Custom resource contains the following information:

* ID of the resource is UUID of the PV
* spec section:
    * Number of desired replicas.
    * Scheduling constraints (preferred/required nodes).
    * Minimum required and maximum allowed size of the volume.
* status section:
    * Overall state of the volume/nexus.
    * Name of the node where the nexus lives.
    * List of replicas each record containing URI, node name and state.
    * The actual size of the volume
    * Information which node is the volume mounted on (if any)

## Selecting pool (node) for a replica

All pools are equal in a sense that we don't distinguish between pools backed
by SSD or HDD devices. When making decision about where to place a replica
following facts are considered (from the most to the least important):

1. Volume affinity constraints
2. State of the pool (should be online)
3. Count of replicas on the pool (the fewer the better)
4. Free space on the pool (the more the better)

## Selecting node for the nexus

Due to late binding of mayastor volumes, we get information about the node
that will host the application using the volume. If local flag in storage
class is set, then we will create the nexus on the same node as the
application. If the local flag is not set, then we try to do the same and
if that is not possible then we pick a node that has the least nexuses.
