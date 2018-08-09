import pytest
from lenses_python.lenses import lenses


@pytest.fixture(autouse=True)
def lenses_conn():
    return lenses("http://localhost:3030", "admin", "admin")
