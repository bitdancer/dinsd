import os

# Run all tests in package for '-m unittest <test_package>'
def load_tests(loader, standard_tests, pattern):
    this_dir = os.path.dirname(__file__)
    if pattern is None:
        pattern = "test*"
    package_tests = loader.discover(start_dir=this_dir, pattern=pattern)
    standard_tests.addTests(package_tests)
    return standard_tests
