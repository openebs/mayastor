"""Common code that represents a mayastor handle."""
from os import name
import grpc
import bdev_pb2 as bdev_pb
import common_pb2 as common_pb
import nexus_pb2 as nexus_pb
import pool_pb2 as pool_pb
import replica_pb2 as replica_pb
import snapshot_pb2 as snapshot_pb
import host_pb2 as host_pb
import bdev_pb2_grpc as bdev_rpc
import nexus_pb2_grpc as nexus_rpc
import pool_pb2_grpc as pool_rpc
import replica_pb2_grpc as replica_rpc
import snapshot_pb2_grpc as snapshot_rpc
import host_pb2_grpc as host_rpc
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
        self.bdev_rpc = bdev_rpc.BdevRpcStub(self.channel)
        self.pool_rpc = pool_rpc.PoolRpcStub(self.channel)
        self.replica_rpc = replica_rpc.ReplicaRpcStub(self.channel)
        self.snapshot_rpc = snapshot_rpc.SnapshotRpcStub(self.channel)
        self.host_rpc = host_rpc.HostRpcStub(self.channel)
        self.nexus_rpc = nexus_rpc.NexusRpcStub(self.channel)
        self._readiness_check()

    def set_timeout(self, timeout):
        self.timeout = timeout

    def install_stub(self, name):
        switcher = {
            "BdevRpcStub": getattr(bdev_rpc, "BdevRpcStub")(self.channel),
            "HostRpcStub": getattr(host_rpc, "HostRpcStub")(self.channel),
            "PoolRpcStub": getattr(pool_rpc, "PoolRpcStub")(self.channel),
            "ReplicaRpcStub": getattr(replica_rpc, "ReplicaRpcStub")(self.channel),
            "SnapshotRpcStub": getattr(snapshot_rpc, "SnapshotRpcStub")(self.channel),
            "NexusRpcStub": getattr(nexus_rpc, "NexusRpcStub")(self.channel),
        }
        stub = switcher[name]

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
        self.bdev_rpc = self.install_stub("BdevRpcStub")
        self.pool_rpc = self.install_stub("PoolRpcStub")
        self.replica_rpc = self.install_stub("ReplicaRpcStub")
        self.snapshot_rpc = self.install_stub("SnapshotRpcStub")
        self.host_rpc = self.install_stub("HostRpcStub")
        self.nexus_rpc = self.install_stub("NexusRpcStub")
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

    def bdev_create(self, uri):
        """create the bdev using the specific URI where URI can be one of the
        following supported schemes:

            nvmf://
            aio://
            uring://
            malloc://

        Note that we do not check the URI schemes, as this should be done in
        mayastor as this is for testing, we do not want to prevent parsing
        invalid schemes."""

        return self.bdev_rpc.Create(bdev_pb.CreateBdevRequest(uri=uri))

    def bdev_share(self, name):
        return self.bdev_rpc.Share(
            bdev_pb.BdevShareRequest(name=str(name), protocol=common_pb.NVMF)
        ).bdev.uri

    def bdev_unshare(self, name):
        return self.bdev_rpc.Unshare(bdev_pb.BdevUnshareRequest(name=str(name)))

    def bdev_destroy(self, uri):
        return self.bdev_rpc.Destroy(bdev_pb.DestroyBdevRequest(uri=str(uri)))

    def bdev_list(self):
        """List all bdevs found within the system."""
        bdev_list = self.bdev_rpc.List(
            bdev_pb.ListBdevOptions(), wait_for_ready=True
        ).bdevs
        return bdev_list

    def pool_create(self, name, uuid, disks):
        """Create a pool with given name on this node using the bdev as the
        backend device. The bdev is implicitly created."""
        opts = pool_pb.CreatePoolRequest(name=name, disks=disks)
        if uuid is not None:
            opts.uuid.value = uuid
        return self.pool_rpc.CreatePool(opts)

    def pool_destroy(self, name):
        """Destroy  the pool."""
        return self.pool_rpc.DestroyPool(pool_pb.DestroyPoolRequest(name=name))

    def pool_list(self, opts=None):
        """Only list pools"""
        if opts == None:
            opts = pool_pb.ListPoolOptions()
        return self.pool_rpc.ListPools(opts, wait_for_ready=True)

    def replica_create(self, pooluuid, name, uuid, size, share=1):
        """Create a replica on the pool with the specified UUID and size."""
        return self.replica_rpc.CreateReplica(
            replica_pb.CreateReplicaRequest(
                pooluuid=pooluuid,
                name=name,
                uuid=str(uuid),
                size=size,
                thin=False,
                share=share,
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
        if opts == None:
            opts = replica_pb.ListReplicaOptions()
        return self.replica_rpc.ListReplicas(opts, wait_for_ready=True)

    def stat_nvme_controllers(self, name):
        """Statistics for all nvmx controllers"""
        return self.host_rpc.StatNvmeControllers(name=name).controllers

    def mayastor_info(self):
        """Get information about Mayastor instance"""
        return self.host_rpc.GetMayastorInfo()

    def nexus_create(
        self, name, uuid, size, min_cntlid, max_cntlid, resv_key, preempt_key, children
    ):
        """Create a nexus with the given name, uuid, size, NVMe controller ID range,
        NVMe reservation and preempt keys for children. The children should be an array
        of nvmf URIs."""
        return self.nexus_rpc.CreateNexus(
            nexus_pb.CreateNexusRequest(
                name=name,
                uuid=str(uuid),
                size=size,
                minCntlId=min_cntlid,
                maxCntlId=max_cntlid,
                resvKey=resv_key,
                preemptKey=preempt_key,
                children=children,
            )
        )

    def nexus_create_snapshot(
        self, nexus_uuid, entity_id, txn_id, snapshot_name, replicas, skip_replicas
    ):
        """Create nexus snapshot"""
        active_replicas = []

        for (r_uuid, s_uuid) in replicas:
            active_replicas.append(
                snapshot_pb.NexusCreateSnapshotReplicaDescriptor(
                    replica_uuid=r_uuid,
                    snapshot_uuid=s_uuid,
                    skip=False,
                )
            )

        args = snapshot_pb.NexusCreateSnapshotRequest(
            nexus_uuid=nexus_uuid,
            entity_id=entity_id,
            txn_id=txn_id,
            snapshot_name=snapshot_name,
            replicas=active_replicas,
        )
        return self.snapshot_rpc.CreateNexusSnapshot(args)

    def nexus_destroy(self, uuid):
        """Destroy the nexus."""
        return self.nexus_rpc.DestroyNexus(nexus_pb.DestroyNexusRequest(uuid=uuid))

    def nexus_publish(self, uuid, share=1):
        """Publish the nexus. this is the same as bdev_share() but is not used
        by the control plane."""
        return self.nexus_rpc.PublishNexus(
            nexus_pb.PublishNexusRequest(uuid=str(uuid), key="", share=share)
        ).nexus.device_uri

    def nexus_unpublish(self, uuid):
        """Unpublish the nexus."""
        return self.nexus_rpc.UnpublishNexus(
            nexus_pb.UnpublishNexusRequest(uuid=str(uuid))
        )

    def nexus_list(self, opts):
        """List all the nexus devices."""
        if opts == None:
            opts = nexus_pb.ListNexusOptions()
        return self.nexus_rpc.ListNexus(opts).nexus_list

    def nexus_add_replica(self, uuid, uri, norebuild):
        """Add a new replica to the nexus"""
        return self.nexus_rpc.AddChildNexus(
            nexus_pb.AddChildNexusRequest(uuid=uuid, uri=uri, norebuild=norebuild)
        )

    def nexus_remove_replica(self, uuid, uri):
        """Add a new replica to the nexus"""
        return self.nexus_rpc.RemoveChildNexus(
            nexus_pb.RemoveChildNexusRequest(uuid=uuid, uri=uri)
        )

    def pools_as_uris(self):
        """Return a list of pools as found on the system."""
        uris = []
        pools = self.pool_rpc.ListPools(pool_pb.ListPoolOptions(), wait_for_ready=True)
        for p in pools.pools:
            uri = "pool://{0}/{1}".format(self.ip_v4, p.name)
            uris.append(uri)
        return uris

    def list_snapshots(self):
        return self.snapshot_rpc.ListSnapshot(snapshot_pb.ListSnapshotsRequest())
