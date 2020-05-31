import pytest
from lensesio.lenses import main


@pytest.fixture(autouse=True)
def lenses_conn():
    return main("basic", "https://localhost:9991", "admin", "admin", verify_cert=False)
