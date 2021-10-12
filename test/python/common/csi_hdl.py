import grpc
import csi_pb2 as pb
import csi_pb2_grpc as rpc


class CsiHandle(object):
    def __init__(self, csi_socket):
        self.channel = grpc.insecure_channel(csi_socket)
        # self.controller = rpc.ControllerStub(self.channel)
        self.identity = rpc.IdentityStub(self.channel)
        self.node = rpc.NodeStub(self.channel)
        self._readiness_check()

    def _readiness_check(self):
        info = self.identity.GetPluginInfo(
            pb.GetPluginInfoRequest(), wait_for_ready=True
        )
        assert info.name == "io.openebs.csi-mayastor"

    def __del__(self):
        self.close()

    def close(self):
        self.channel.close()
