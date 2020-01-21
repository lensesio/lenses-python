import pytest
from lensesio.lenses import main


@pytest.fixture(autouse=True)
def lenses_conn():
    return main("basic", "http://localhost:3030", "admin", "admin")
