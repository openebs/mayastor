# Control Plane Behaviour

This document describes the types of behaviour that the control plane will exhibit under various situations. By
providing a high-level view it is hoped that the reader will be able to more easily reason about the control plane. \
<br>

## REST API Idempotency

Idempotency is a term used a lot but which is often misconstrued. The following definition is taken from
the [Mozilla Glossary](https://developer.mozilla.org/en-US/docs/Glossary/Idempotent):

> An [HTTP](https://developer.mozilla.org/en-US/docs/Web/HTTP) method is **idempotent** if an identical request can be
> made once or several times in a row with the same effect while leaving the server in the same state. In other words,
> an idempotent method should not have any side-effects (except for keeping statistics). Implemented correctly, the `GET`,
`HEAD`,`PUT`, and `DELETE` methods are idempotent, but not the `POST` method.
> All [safe](https://developer.mozilla.org/en-US/docs/Glossary/Safe) methods are also ***idempotent***.

OK, so making multiple identical requests should produce the same result ***without side effects***. Great, so does the
return value for each request have to be the same? The article goes on to say:

> To be idempotent, only the actual back-end state of the server is considered, the status code returned by each request
> may differ: the first call of a `DELETE` will likely return a `200`, while successive ones will likely return a`404`.

The control plane will behave exactly as described above. If, for example, multiple `create volume` calls are made for
the same volume, the first will return success (`HTTP 200` code) while subsequent calls will return a failure status
code (`HTTP 409` code) indicating that the resource already exists. \
<br>

## Handling Failures

There are various ways in which the control plane could fail to satisfy a `REST` request:

- Control plane dies in the middle of an operation.
- Control plane fails to update the persistent store.
- A gRPC request to Mayastor fails to complete successfully. \
  <br>

Regardless of the type of failure, the control plane has to decide what it should do:

1. Fail the operation back to the callee but leave any created resources alone.

2. Fail the operation back to the callee but destroy any created resources.

3. Act like kubernetes and keep retrying in the hope that it will eventually succeed. \
<br>

Approach 3 is discounted. If we never responded to the callee it would eventually timeout and probably retry itself.
This would likely present even more issues/complexity in the control plane.

So the decision becomes, should we destroy resources that have already been created as part of the operation? \
<br>

### Keep Created Resources

Preventing the control plane from having to unwind operations is convenient as it keeps the implementation simple. A
separate asynchronous process could then periodically scan for unused resources and destroy them.

There is a potential issue with the above described approach. If an operation fails, it would be reasonable to assume
that the user would retry it. Is it possible for this subsequent request to fail as a result of the existing unused
resources lingering (i.e. because they have not yet been destroyed)? If so, this would hamper any retry logic
implemented in the upper layers.

### Destroy Created Resources

This is the optimal approach. For any given operation, failure results in newly created resources being destroyed. The
responsibility lies with the control plane tracking which resources have been created and destroying them in the event
of a failure.

However, what happens if destruction of a resource fails? It is possible for the control plane to retry the operation
but at some point it will have to give up. In effect the control plane will do its best, but it cannot provide any
guarantee. So does this mean that these resources are permanently leaked? Not necessarily. Like in
the [Keep Created Resources](#keep-created-resources) section, there could be a separate process which destroys unused
resources. \
<br>

## Use of the Persistent Store

For a control plane to be effective it must maintain information about the system it is interacting with and take
decision accordingly. An in-memory registry is used to store such information.

Because the registry is stored in memory, it is volatile - meaning all information is lost if the service is restarted.
As a consequence critical information must be backed up to a highly available persistent store (for more detailed
information see [persistent-store.md](./control-plane.md#persistent-store-kvstore-for-configuration-data)).

The types of data that need persisting broadly fall into 3 categories:

1. Desired state

2. Actual state

3. Control plane specific information \
   <br>

### Desired State

This is the declarative specification of a resource provided by the user. As an example, the user may request a new
volume with the following requirements:

- Replica count of 3

- Size

- Preferred nodes

- Number of nexuses

Once the user has provided these constraints, the expectation is that the control plane should create a resource that
meets the specification. How the control plane achieves this is of no concern.

So what happens if the control plane is unable to meet these requirements? The operation is failed. This prevents any
ambiguity. If an operation succeeds, the requirements have been met and the user has exactly what they asked for. If the
operation fails, the requirements couldn’t be met. In this case the control plane should provide an appropriate means of
diagnosing the issue i.e. a log message.

What happens to resources created before the operation failed? This will be dependent on the chosen failure strategy
outlined in [Handling Failures](#handling-failures).

### Actual State

This is the runtime state of the system as provided by Mayastor. Whenever this changes, the control plane must reconcile
this state against the desired state to ensure that we are still meeting the users requirements. If not, the control
plane will take action to try to rectify this.

Whenever a user makes a request for state information, it will be this state that is returned (Note: If necessary an API
may be provided which returns the desired state also). \
<br>

## Control Plane Information

This information is required to aid the control plane across restarts. It will be used to store the state of a resource
independent of the desired or actual state.

The following sequence will be followed when creating a resource:

1. Add resource specification to the store with a state of “creating”

2. Create the resource

3. Mark the state of the resource as “complete”

If the control plane then crashes mid-operation, on restart it can query the state of each resource. Any resource not in
the “complete” state can then be destroyed as they will be remnants of a failed operation. The expectation here will be
that the user will reissue the operation if they wish to.

Likewise, deleting a resource will look like:

1. Mark resources as “deleting” in the store

2. Delete the resource

3. Remove the resource from the store.

For complex operations like creating a volume, all resources that make up the volume will be marked as “creating”. Only
when all resources have been successfully created will their corresponding states be changed to “complete”. This will
look something like:

1. Add volume specification to the store with a state of “creating”

2. Add nexus specifications to the store with a state of “creating”

3. Add replica specifications to the store with a state of “creating”

4. Create replicas

5. Create nexus

6. Mark replica states as “complete”

7. Mark nexus states as “complete”

8. Mark volume state as “complete”
