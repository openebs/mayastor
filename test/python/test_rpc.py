from common.command import run_cmd
from common.mayastor import container_mod, mayastor_mod
import pytest
import grpc
import uuid as guid
import asyncio


@pytest.fixture
def pool_file():
    return "/var/tmp/pool1.img"


@pytest.fixture
def create_temp_file(pool_file):
    run_cmd("rm -f {}".format(pool_file))
    run_cmd("truncate -s 3G {}".format(pool_file), True)
    yield
    run_cmd("rm -f {}".format(pool_file))


@pytest.mark.asyncio
async def test_rpc_timeout(container_mod, mayastor_mod, create_temp_file, pool_file):
    ms1_c = container_mod.get("ms1")
    ms1 = mayastor_mod.get("ms1")
    uuid = str(guid.uuid4())

    timeout_pattern = 'destroy_replica: gRPC method timed out, args: DestroyReplicaRequest {{ uuid: "{}" }}'.format(
        uuid
    )

    # Create a pool and a big replica (> 1 GB)
    ms1.pool_create("pool1", "uring://{}".format(pool_file))
    ms1.replica_create("pool1", uuid, 2 * 1024 * 1024 * 1024)

    # Set timeout to the minimum possible value and reconnect handles.
    ms1.set_timeout(1)
    ms1.reconnect()

    # Destroy the replica and trigger the timeout.
    with pytest.raises(grpc.RpcError) as error:
        ms1.replica_destroy(uuid)
    assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT

    # Should not see error message pattern, as we expect the call to be timed out.
    assert str(ms1_c.logs()).find(timeout_pattern) == -1

    # Try to destroy the replica one more time - the call should complete
    # without assertions.
    # We expect this call to detect the incompleted previous call and emit
    # a warning.
    ms1.replica_destroy(uuid)

    # Now we should see the evidence that the gRPC call was timed out.
    assert str(ms1_c.logs()).find(timeout_pattern) > 0
