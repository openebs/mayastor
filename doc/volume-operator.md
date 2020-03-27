# Volume Operator

Volume operator is part of MayaStor control plane (MOAC) and is responsible
for volume creation, destruction and recovery of volumes on storage nodes.
By volume, we mean "persistent volume" in K8s, which is dynamically created
as a result of creating "persistent volume claim". A volume consists of
a nexus and replica(s). Nexus is a logical volume, entry point for IOs, which
are further dispatched to replicas - physical volumes, which allocate space
from storage pools and can be accessed either locally (in the same process
where nexus lives) or using remote storage protocol NVMEoF.

## Phase one (stateless)

A goal of phase one was to manage volumes based on CSI requests received
from k8s without maintaining a volume state in the cluster database. The volume
operator relies solely on information:

* received from CSI requests (i.e. create volume parameters from CSI RPC method)
* obtained from storage nodes (i.e. list of created replicas and their parameters)

The advantage of this solution is its simplicity. When moac starts up it reads
all available information from storage nodes and then tries to create/modify
objects according to what it believes is the desired state. For example, if
it finds a nexus with three replicas but only two replicas exist, it will
create the third one. If it finds three replicas belonging to the same volume
but no nexus, then it creates the nexus and connects it to the replicas.
That works, though not in all cases. Consider the following scenarios:

### Case 1: storage node is offline

We can neither obtain information from it nor can we rely on previously
stored information in memory (moac could have been just restarted).
We don't know about nexus's and replicas located on the node. There
are two possible kinds of losses when the node is down:

1. Replica is lost: In such case, we can deduce the desired number of
   replicas from nexus configuration and start a recovery.

2. Nexus (and local replica) is lost: We cannot start a recovery
   because we don't know the desired replica count.

The second type of loss looks like a problem, but given the current state of
implementation it is not a real problem. Nexus runs on the same node
as the application. If the node is down, so the app using it.
When the node comes online again, so the nexus. k8s cannot reschedule
the app while the node is down because the volume constraints disallow
that. It will become a problem when accessing nexus over NVMEoF on
a different node. Knowing that a nexus is unavailable and parameters
needed to recreate it on a different node are necessary for proper
recovery.

### Case 2: Orphaned volume

CSI tells us when to create or destroy volume, but it does not tell us
if a replica or nexus found on a storage node has corresponding
persistent volume object in k8s. Without removing such an object we risk
a disk space leak. If we remove the object we risk a data loss.
Safer seems to let user decide what to do with the volume. However for
that the user needs to have a resource in k8s representing the volume,
so that he can inspect the resource and potentially delete it.

### Case 3: Scaling replicas up and down

The number of replicas is given in storage class and passed as a parameter
to volume create RPC method. How can we change the replica count on
existing volumes? Updating storage class applies only to new replicas
(needs to confirm). PVs and PVCs are not watched by volume operator
(only by CSI implementation in k8s). Having a resource representing
a volume with a replica count parameter that could be updated sounds like
a solution.

### Case 4: Node affinity rules when a new replica is added

When a volume is created using CSI method it is possible to specify
two kinds of basic criteria for node selection (the node used for nexus):

1. must run on node xxx
2. would be nice to run on node xxx

Unfortunately without saving these parameters to persistent storage,
they are not available when recovering failed volume. So it could
happen that we schedule nexus or replica on a wrong node.

### Case 5: State of the volume

How to communicate information about the state of the volume back to a user?
Updating PV records in k8s does not seem practical. Which field should
that be? An alternative way would be to update a custom resource that is
under the control of moac. The state information may include: overall
volume status, the status of each individual replica, the progress of rebuild
in percents or even such basic information like which node nexus/replica
reside on. Along with `kubectl` get/describe command this makes the
custom resource a perfect way how to gain an insight into the mayastor.

## Phase two (stateful)

Maintaining a volume state in etcd means that the information about the
volumes will be largely duplicated. Custom resources representing volumes
reflect persistent volumes. Though the duplication of data solves problems
that would be otherwise difficult or impossible to solve as we have shown
above. The description of resource properties follows.

### Mayastor Volume Custom Resource

The "msv" resource is created when the volume is created (using CSI) and
destroyed when the volume is destroyed using CSI or the custom resource
is deleted by kubectl. Creating or modifying the resource directly by
users (i.e. using kubectl) is discouraged except for the replica count
parameter. All nexus's and replicas without corresponding custom resource
are subject to immediate removal. Custom resource contains the following
information:

* ID of the resource is UUID of the PV
* spec section:
    * Number of desired replicas.
    * Scheduling constraints (preferred/required nodes).
    * Minimum required and maximum allowed size of the volume.
* status section:
    * Overall state of the volume/nexus.
    * Name of the node where the nexus lives.
    * List of replicas each record containing URI, node name and state.
    * Size of the volume

Status section contains volatile data which are subject to change.
The rest of the attributes are part of volume specification and are not
expected to change except when the user explicitly modifies them.

### Concerns

1. What if the user removes msv for a volume that is still in use (has PV)?
