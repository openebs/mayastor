"""Common code that represents a mayastor handle."""
import mayastor_pb2 as pb
import grpc
import mayastor_pb2_grpc as rpc
from pytest_testconfig import config
from functools import partial

pytest_plugins = ["docker_compose"]


class MayastorHandle(object):
    """Mayastor gRPC handle."""

    def __init__(self, ip_v4):
        """Init."""
        self.ip_v4 = ip_v4
        self.timeout = float(config["grpc"]["client_timeout"])
        self.channel = grpc.insecure_channel(("%s:10124") % self.ip_v4)
        self.bdev = rpc.BdevRpcStub(self.channel)
        self.ms = rpc.MayastorStub(self.channel)
        self._readiness_check()

    def set_timeout(self, timeout):
        self.timeout = timeout

    def install_stub(self, name):
        stub = getattr(rpc, name)(self.channel)

        # Install default timeout to all functions, ignore system attributes.
        for f in dir(stub):
            if not f.startswith("__"):
                h = getattr(stub, f)
                setattr(stub, f, partial(h, timeout=self.timeout))
        return stub

    def _readiness_check(self):
        try:
            self.bdev_list()
            self.pool_list()
        except grpc._channel._InactiveRpcError:
            # This is to get around a gRPC bug.
            # Retry once before failing
            self.bdev_list()
            self.pool_list()

    def reconnect(self):
        self.channel = grpc.insecure_channel(("%s:10124") % self.ip_v4)
        self.bdev = self.install_stub("BdevRpcStub")
        self.ms = self.install_stub("MayastorStub")
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

        return self.bdev.Create(pb.BdevUri(uri=uri))

    def bdev_share(self, name):
        return self.bdev.Share(pb.BdevShareRequest(name=str(name), proto="nvmf")).uri

    def bdev_unshare(self, name):
        return self.bdev.Unshare(pb.CreateReply(name=str(name)))

    def bdev_destroy(self, uri):
        return self.bdev.Destroy(pb.BdevUri(uri=str(uri)))

    def pool_create(self, name, bdev):
        """Create a pool with given name on this node using the bdev as the
        backend device. The bdev is implicitly created."""

        disks = []
        disks.append(bdev)
        return self.ms.CreatePool(pb.CreatePoolRequest(name=name, disks=disks))

    def pool_destroy(self, name):
        """Destroy  the pool."""
        return self.ms.DestroyPool(pb.DestroyPoolRequest(name=name))

    def replica_create(self, pool, uuid, size, share=1):
        """Create  a replica on the pool with the specified UUID and size."""
        return self.ms.CreateReplica(
            pb.CreateReplicaRequest(
                pool=pool, uuid=str(uuid), size=size, thin=False, share=share
            )
        )

    def replica_create_v2(self, pool, name, uuid, size, share=1):
        """Create a replica on the pool with the specified UUID and size."""
        return self.ms.CreateReplicaV2(
            pb.CreateReplicaRequestV2(
                pool=pool, name=name, uuid=uuid, size=size, thin=False, share=share
            )
        )

    def replica_destroy(self, uuid):
        """Destroy the replica by the UUID, the pool is resolved within
        mayastor."""
        return self.ms.DestroyReplica(pb.DestroyReplicaRequest(uuid=uuid))

    def replica_list(self):
        """List existing replicas"""
        return self.ms.ListReplicas(pb.Null())

    def replica_list_v2(self):
        """List existing replicas along with their UUIDs"""
        return self.ms.ListReplicasV2(pb.Null())

    def nexus_create(self, uuid, size, children):
        """Create a nexus with the given uuid and size. The children should
        be an array of nvmf URIs."""
        return self.ms.CreateNexus(
            pb.CreateNexusRequest(uuid=str(uuid), size=size, children=children)
        )

    def nexus_create_v2(
        self, name, uuid, size, min_cntlid, max_cntlid, resv_key, preempt_key, children
    ):
        """Create a nexus with the given name, uuid, size, NVMe controller ID range,
        NVMe reservation and preempt keys for children. The children should be an array
        of nvmf URIs."""
        return self.ms.CreateNexusV2(
            pb.CreateNexusV2Request(
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

    def nexus_destroy(self, uuid):
        """Destroy the nexus."""
        return self.ms.DestroyNexus(pb.DestroyNexusRequest(uuid=uuid))

    def nexus_publish(self, uuid, share=1):
        """Publish the nexus. this is the same as bdev_share() but is not used
        by the control plane."""
        return self.ms.PublishNexus(
            pb.PublishNexusRequest(uuid=str(uuid), key="", share=share)
        ).device_uri

    def nexus_unpublish(self, uuid):
        """Unpublish the nexus."""
        return self.ms.UnpublishNexus(pb.UnpublishNexusRequest(uuid=str(uuid)))

    def nexus_list(self):
        """List all the nexus devices."""
        return self.ms.ListNexus(pb.Null()).nexus_list

    def nexus_list_v2(self):
        """List all the nexus devices, with separate name and uuid."""
        return self.ms.ListNexusV2(pb.Null()).nexus_list

    def nexus_add_replica(self, uuid, uri, norebuild):
        """Add a new replica to the nexus"""
        return self.ms.AddChildNexus(
            pb.AddChildNexusRequest(uuid=uuid, uri=uri, norebuild=norebuild)
        )

    def nexus_remove_replica(self, uuid, uri):
        """Add a new replica to the nexus"""
        return self.ms.RemoveChildNexus(pb.RemoveChildNexusRequest(uuid=uuid, uri=uri))

    def bdev_list(self):
        """List all bdevs found within the system."""
        return self.bdev.List(pb.Null(), wait_for_ready=True).bdevs

    def pool_list(self):
        """Only list pools"""
        return self.ms.ListPools(pb.Null(), wait_for_ready=True)

    def pools_as_uris(self):
        """Return a list of pools as found on the system."""
        uris = []
        pools = self.ms.ListPools(pb.Null(), wait_for_ready=True)
        for p in pools.pools:
            uri = "pool://{0}/{1}".format(self.ip_v4, p.name)
            uris.append(uri)
        return uris

    def stat_nvme_controllers(self):
        """Statistics for all nvmx controllers"""
        return self.ms.StatNvmeControllers(pb.Null()).controllers

    def mayastor_info(self):
        """Get information about Mayastor instance"""
        return self.ms.GetMayastorInfo(pb.Null())
