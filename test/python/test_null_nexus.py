from common.command import run_cmd_async_at
from common.nvme import nvme_remote_connect, nvme_remote_disconnect
from common.fio import Fio
import pytest
import asyncio
import mayastor_pb2 as pb
# Reusing nexus UUIDs to avoid the need to disconnect between tests
nexus_uuids = [
    "78c0e836-ef26-47c2-a136-2a99b538a9a8",
    "fc2bd1bf-301c-46e7-92e7-71e7a062e2dd",
    "1edc6a04-74b0-450e-b953-14237a6795de",
    "70bb42e6-4924-4079-a755-3798015e1319",
    "9aae12d7-48dd-4fa6-a554-c4d278a9386a",
    "af8107b6-2a9b-4097-9676-7a0941ba9bf9",
    "7fde42a4-758b-466e-9755-825328131f67",
    "6956b466-7491-4b8f-9a97-731bf7c9fd9c",
    "c5fa226d-b1c7-4102-83b3-7f69ff1b311b",
    "0cc74b98-fcd9-4a95-93ea-7c921d56c8cc",
    "460758a4-d87f-4738-8666-cd07c23077db",
    "63c567c9-75a6-466f-940e-a4ec5163794d",
    "e425b05c-0730-4c10-8d01-77a2a65b4016",
    "38d73fea-6f69-4a80-8959-2e2705be0f52",
    "7eae6f7c-a52a-4689-b591-9642a094b4cc",
    "52978839-8813-4c38-8665-42a78eeab499",
    "8dccd362-b0fa-473d-abd3-b9c4d2a95f48",
    "41dd24c4-8d20-4ee7-b52d-45dfcd580c52",
    "9d879d46-8f71-4520-8eac-2b749c76adb8",
    "d53cf04b-032d-412d-822a-e6b8b308bc52",
    "da6247ab-6c28-429b-8848-c290ee474a81",
    "71e9aab8-a350-4768-ab56-a2c66fda4e80",
    "2241726a-487f-4852-8735-ddf849c92145",
    "dbbbd8d4-96a4-45ae-9403-9dd43e34db6d",
    "51ceb351-f864-43fc-bf31-3e36f75d8e86",
    "7f90415a-29b3-41cd-9918-2d35b3b66056",
    "f594f29c-b227-46a7-b4a6-486243c9f500",
    "563b91ab-7ffd-44fc-aead-c551e280587a",
    "8616f575-bcc9-490e-9524-9e6e7d9c76cc",
    "817d4ca0-1f52-40de-b3d2-e59ce28b05ee",
    "0a1103de-c466-4f77-88ca-521044b18483",
    "ef6ff58b-0307-43df-bb87-9db0d06c1818",
    "615c6fbb-90c1-46d6-b47c-6436a023953d",
    "201e80b9-9389-4013-ab3c-b85b40d0cb56",
    "e392187b-657f-4b4b-a249-cae27b7a5ba5",
    "19e34f44-ff93-437d-9c11-31adfc974d64",
    "b9542cb0-12e9-4b32-9cab-3b476161e1c6",
    "3db3bfb9-0453-48bf-bb57-bd51b35e7f77",
    "10e1f9e8-4cb2-4a79-a3d4-2b6314b58ba7",
    "5ab8188a-e622-4965-b558-3355a5ceb285",
    "063e2338-c42a-4aee-b4ed-b97a6c3bdc2b",
    "94b03db5-14d7-4668-a952-e71160a658fc",
    "4d03a0bc-645c-45ce-8d5e-a9ca584dfcb0",
    "1a038ddb-fb0d-45b3-a46c-bdd57030af9e",
    "89eecdef-4dc7-4228-8794-7139f15bf966",
    "67369dd2-9c6a-49f8-b7bb-32ecba8698f2",
    "f57cc434-d00c-4fee-b85f-56126403bf31",
    "f9458cf7-8a12-487c-88f2-19c89e1d60c5",
    "a33aca3e-fa5f-4477-b945-78616316ffb0",
    "965329ba-24c1-4de7-b988-5a0baa376e66",
    "453adc9f-501e-4d03-8810-990b747296e3",
    "3a95e49d-afaa-4f3f-871a-4ec96ab86380",
    "710450f3-266a-462a-abc0-bd3cdf235568",
    "619b8ec8-2098-47fc-a55c-0a4048687163",
    "9e3ae3ee-ddfe-4d81-93c0-9c62737d96fb",
    "bc320f97-3a1f-4c6f-a2ee-936bfb5f293c",
    "e5e271a8-d099-4cf4-8035-1f1672b6b16e",
    "0fae6293-57b6-4135-b7dc-317b210d89b6",
    "b8debec5-ea8e-4063-bba9-630bd108752f",
    "cab0e91e-4e27-4820-a734-06c0bcd3f6ae",
    "4986c804-64e9-4fb9-93ce-8ad6ca0cd3b2",
    "5604d2cd-8ba6-4322-900a-31396168b72c",
    "1affafb6-2089-45b5-8938-e40de8f89381",
    "1fc64e79-9875-4136-b312-9f5df02d7c93",
    "7fe16343-40dd-4bb5-bc63-9021be0cafb7",
    "d24ad88e-b4ed-4ca5-91a0-b7afbc99744a",
    "65889c75-7b2b-40a6-bfec-7bd9db64d20a",
    "f60c9d96-360c-4b50-a1a8-f3fce87e24d7",
    "4b6dc95f-1fb2-47f5-9746-e0e3970cbbb3",
    "b37eb168-6430-44f8-8242-d1e0110bc71e",
    "e34264f2-c999-4a3e-b2af-53b0c4f45095",
    "157e6489-a96c-4e8c-8843-928c89529fff",
    "efcbca04-8b0b-4a48-b3f2-e644a732d16d",
    "238e35f2-9baa-4540-8fbd-ee46d2eca2cc",
    "2f7e6ffb-47d5-485e-9166-1d455f2ec824",
    "f75099a7-8600-4e4e-8332-1d045b6b85e1",
    "2323b974-420c-40f7-8296-a28c4bc6b64e",
    "31e7dab5-dbcb-4c33-999f-0e6779dad480",
    "5221023d-6a15-4eeb-bf82-b770fcf8576d",
    "51eee369-d85f-4097-ab93-419d31c2205f",
    "3beaf7e5-a70e-4687-a28f-f9c8ff9364d8",
    "c80d88bb-b1ca-454f-b793-19b00b141479",
    "fda3e343-1f29-4e4d-8e56-6dd77fe28838",
    "298a065a-1571-434e-a8cd-7464890595be",
    "64540a85-4260-4469-81be-fac0e831a0ad",
    "1fc17318-cec1-40cd-86d4-f498ce3342a4",
    "30096c80-6e35-4c3f-910b-99b4190b79e1",
    "4451d707-39d9-4174-b7ea-8dcfc4c408d4",
    "dbc05fa6-bd30-4e0d-997b-6f8cb8854141",
    "4ce06ba7-9074-445d-b97b-d665f065d60e",
    "80115d98-b7df-4ed2-8fd8-7c7464143ce4",
    "aa7142fc-b6c3-4499-98a2-5f424912d107",
    "8adf8819-c3eb-43ce-ad11-04e0d22dfb52",
    "a21b7d6e-354e-4d2d-b75f-b5782dcef385",
    "b7ac8c80-8dfa-4314-8d76-6a57f87ad32f",
    "2b15ccf1-6ee2-4c7d-9286-5133b0b57844",
    "f443ce61-8ba8-4490-8bf9-8c1c443a0aa2",
    "73025002-8582-48f4-8e32-d9866e8d97d2",
    "6eb21022-4a99-4dd8-b76f-f2715378253b",
    "f0e074af-2f97-4c67-bac8-f29f409b9db2",
]


def check_nexus_state(ms, state=pb.NEXUS_ONLINE):
    nl = ms.nexus_list()
    for nexus in nl:
        assert nexus.state == state
        for child in nexus.children:
            assert child.state == pb.CHILD_ONLINE


def destroy_nexus(ms, list):
    for uuid in list:
        ms.nexus_destroy(uuid)


@pytest.fixture
def create_nexus_devices(mayastors, share_null_devs):

    rlist_m0 = mayastors.get('ms0').bdev_list()
    rlist_m1 = mayastors.get('ms1').bdev_list()
    rlist_m2 = mayastors.get('ms2').bdev_list()

    assert len(rlist_m0) == len(rlist_m1) == len(rlist_m2)

    ms = mayastors.get('ms3')

    uris = []

    for uuid in nexus_uuids:
        ms.nexus_create(uuid, 94 * 1024 * 1024, [
            rlist_m0.pop().share_uri,
            rlist_m1.pop().share_uri,
            rlist_m2.pop().share_uri
        ])

    for uuid in nexus_uuids:
        uri = ms.nexus_publish(uuid)
        uris.append(uri)

    assert len(ms.nexus_list()) == len(nexus_uuids)

    return uris


@pytest.fixture
def create_null_devs(mayastors):
    for node in ['ms0', 'ms1', 'ms2']:
        ms = mayastors.get(node)

        for i in range(len(nexus_uuids)):
            ms.bdev_create(f"null:///null{i}?blk_size=512&size_mb=100")


@pytest.fixture
def share_null_devs(mayastors, create_null_devs):

    for node in ['ms0', 'ms1', 'ms2']:
        ms = mayastors.get(node)
        names = ms.bdev_list()
        for n in names:
            ms.bdev_share((n.name))


async def kill_after(container, sec):
    """Kill the given container after sec seconds."""
    await asyncio.sleep(sec)
    container.kill()


async def connect_to_uris(uris, target_vm):
    devices = []

    for uri in uris:
        dev = await nvme_remote_connect(target_vm, uri)
        devices.append(dev)

    job = Fio("job1", "randwrite", devices).build()

    return job


async def disconnect_from_uris(uris, target_vm):
    for uri in uris:
        await nvme_remote_disconnect(target_vm, uri)


@pytest.mark.asyncio
async def test_null_nexus(create_nexus_devices, mayastors, target_vm):
    vm = target_vm
    uris = create_nexus_devices
    ms = mayastors.get('ms3')
    check_nexus_state(ms)

    # removing the last three lines will allow you to do whatever against
    # the containers at this point.
    job = await connect_to_uris(uris, vm)
    await run_cmd_async_at(vm, job)
    await disconnect_from_uris(uris, vm)
