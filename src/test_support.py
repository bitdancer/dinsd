import os

# Run all tests in package for '-m unittest <test_package>'
def load_tests(loader, standard_tests, pattern):
    this_dir = os.path.dirname(__file__)
    if pattern is None:
        pattern = "test*"
    package_tests = loader.discover(start_dir=this_dir, pattern=pattern)
    standard_tests.addTests(package_tests)
    return standard_tests

# We need these classes defined in a real module in order for pickling
# to work.

from dinsd import Scaler
class ID(Scaler):

    def __init__(self, id):
        if isinstance(id, self.__class__):
            self.value = id.value
            return
        if not isinstance(id, str):
            raise TypeError(
                "Expected str but passed {}".format(type(id)))
        if (2 <= len(id) <=4 and id.startswith(self.firstchar) and
                id[1:].isdigit()):
            self.value = id
        else:
            raise TypeError("Expected '{}' followed by one to "
                            "three digits, got {!r}".format(
                            self.firstchar, id))

class SID(ID):
    firstchar = 'S'

class CID(ID):
    firstchar = 'C'
