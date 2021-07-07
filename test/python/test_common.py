from common.hdl import MayastorHandle
from common.mayastor import mayastor_mod
import pytest


@pytest.mark.asyncio
async def test_mayastor_features(mayastor_mod):
    ms1 = mayastor_mod.get("ms1")
    ms3 = mayastor_mod.get("ms3")

    for replica, ms in ((True, ms1), (False, ms3)):
        ms_info = ms.mayastor_info()

        assert ms_info.version.startswith("v0.")

        # Should see ANA disabled on mayastors where environment
        # variable is not set.
        features = ms_info.supportedFeatures
        if replica:
            assert features.asymmetricNamespaceAccess == False
        else:
            assert features.asymmetricNamespaceAccess == True
