"""Common code that represents a mayastor handle."""
from os import name
import grpc
import pool_pb2 as pool_pb
import replica_pb2 as replica_pb
import pool_pb2_grpc as pool_rpc
import replica_pb2_grpc as replica_rpc
from pytest_testconfig import config
from functools import partial

from google.protobuf import struct_pb2 as google_dot_protobuf_dot_struct__pb2

pytest_plugins = ["docker_compose"]


class MayastorHandle(object):
    """Mayastor gRPC handle."""

    def __init__(self, ip_v4):
        """Init."""
        self.ip_v4 = ip_v4
        self.timeout = float(config["grpc"]["client_timeout"])
        self.channel = grpc.insecure_channel(("%s:10124") % self.ip_v4)
        self.pool_rpc = pool_rpc.PoolRpcStub(self.channel)
        self.replica_rpc = replica_rpc.ReplicaRpcStub(self.channel)
        self._readiness_check()

    def set_timeout(self, timeout):
        self.timeout = timeout

    def install_stub(self, name):
        stub = getattr(pool_rpc, name)(self.channel)
        stub = getattr(replica_rpc, name)(self.channel)

        # Install default timeout to all functions, ignore system attributes.
        for f in dir(stub):
            if not f.startswith("__"):
                h = getattr(stub, f)
                setattr(stub, f, partial(h, timeout=self.timeout))
        return stub

    def _readiness_check(self):
        try:
            self.pool_list(pool_pb.ListPoolOptions())
        except grpc._channel._InactiveRpcError:
            # This is to get around a gRPC bug.
            # Retry once before failing
            self.pool_list(pool_pb.ListPoolOptions())

    def reconnect(self):
        self.channel = grpc.insecure_channel(("%s:10124") % self.ip_v4)
        self.pool_rpc = self.install_stub("PoolRpcStub")
        self.replica_rpc = self.install_stub("ReplicaRpcStub")
        self._readiness_check()

    def __del__(self):
        del self.channel

    def close(self):
        self.__del__()

    def ip_address(self):
        return self.ip_v4

    def as_target(self) -> str:
        """Returns this node as scheme which is used to designate this node to
        be used as the node where the nexus shall be created on."""
        node = "nvmt://{0}".format(self.ip_v4)
        return node

    def pool_create(self, name, bdev):
        """Create a pool with given name on this node using the bdev as the
        backend device. The bdev is implicitly created."""
        disks = []
        disks.append(bdev)
        return self.pool_rpc.CreatePool(
            pool_pb.CreatePoolRequest(name=name, disks=disks)
        )

    def pool_destroy(self, name):
        """Destroy  the pool."""
        return self.pool_rpc.DestroyPool(pool_pb.DestroyPoolRequest(name=name))

    def pool_list(self, opts):
        """Only list pools"""
        return self.pool_rpc.ListPools(opts, wait_for_ready=True)

    def replica_create(self, pooluuid, name, uuid, size, share=1):
        """Create a replica on the pool with the specified UUID and size."""
        return self.replica_rpc.CreateReplica(
            replica_pb.CreateReplicaRequest(
                pooluuid=pooluuid, name=name, uuid=uuid, size=size, thin=False, share=share
            )
        )

    def replica_destroy(self, uuid):
        """Destroy the replica by the UUID, the pool is resolved within
        mayastor."""
        return self.replica_rpc.DestroyReplica(
            replica_pb.DestroyReplicaRequest(uuid=uuid)
        )

    def replica_list(self, opts):
        """List existing replicas along with their UUIDs"""
        return self.replica_rpc.ListReplicas(opts, wait_for_ready=True)
