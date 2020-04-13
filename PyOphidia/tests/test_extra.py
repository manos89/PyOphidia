import pytest
from extra import *
from PyOphidia import cube

cube.Cube.setclient()
cube1 = cube.Cube(src_path='/public/data/ecas_training/tasmax_day_CMCC-CESM_rcp85_r1i1p1_20960101-21001231.nc',
                 measure='tasmax',
                 import_metadata='yes',
                 imp_dim='time',
                 imp_concept_level='d', vocabulary='CF', hierarchy='oph_base|oph_base|oph_time',
                 ncores=4,
                 description='Max Temps'
                 )
cube2 = cube.Cube.randcube(container="mytest", dim="lat|lon|time", dim_size="4|2|1", exp_ndim=2,
                                   host_partition="main", measure="tos", measure_type="double", nfrag=4, ntuple=2,
                                   nhost=1)

@pytest.mark.parametrize(("cube", "operation", "dim", "frequency", "midnight", "expected"),
                         [(cube1, "sum", "implicit", None, None, "oph_reduce"),
                          (cube2, "acmoment", "implicit", "A", None, "oph_reduce"),
                          (cube1, "sum", "explicit", None, None, "oph_aggregate"),
                          (cube1, "std", "lat", None, None, "oph_reduce2"),
                          (cube1, "sum", "time", "A", None, "oph_aggregate2"),
                          (cube1, "count", "lon", None, None, "oph_reduce2"),
                          (cube1, "min", "time", "q", None, "oph_aggregate2"),
                          (cube1, "avg", "time", None, None, "oph_reduce2"),
                          (cube1, "acmoment", "explicit", None, None, "oph_aggregate")])
def test_reduction(cube, operation, dim, frequency, midnight, expected):
    reduction(cube=cube, operation=operation, dim=dim, frequency=frequency, midnight=midnight)
    assert cube.client.last_request.split(" ")[0] == expected