import grpc
import csi_pb2 as pb
import csi_pb2_grpc as rpc


class CsiHandle(object):
    def __init__(self, csi_socket):
        self.channel = grpc.insecure_channel(csi_socket)
        # self.controller = rpc.ControllerStub(self.channel)
        self.identity = rpc.IdentityStub(self.channel)
        self.node = rpc.NodeStub(self.channel)

    def __del__(self):
        del self.channel

    def close(self):
        self.__del__()
