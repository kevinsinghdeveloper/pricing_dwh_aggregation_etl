# Testing Guide

## Running Tests

### Using pytest

The recommended way to run tests is using pytest:

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest test/test_pricing_aggregation_etl.py

# Run specific test class
pytest test/test_pricing_aggregation_etl.py::TestPricingAggregationETL

# Run specific test method
pytest test/test_pricing_aggregation_etl.py::TestPricingAggregationETL::test_initialization

# Run with coverage report
pytest --cov=PricingAggregationETL --cov-report=html
```

### Using unittest

You can also run tests using Python's built-in unittest:

```bash
# Run all tests
python -m unittest discover -s test -p "test_*.py"

# Run specific test file
python -m unittest test.test_pricing_aggregation_etl

# Run specific test class
python -m unittest test.test_pricing_aggregation_etl.TestPricingAggregationETL

# Run specific test method
python -m unittest test.test_pricing_aggregation_etl.TestPricingAggregationETL.test_initialization
```

### Using PyCharm

To run tests in PyCharm:

1. Right-click on the `test` directory or a test file
2. Select "Run 'pytest in test'" or "Run 'Unittests in test...'"
3. PyCharm will automatically discover and run the tests

If you encounter import errors in PyCharm:

1. Go to **File > Settings > Project > Project Structure**
2. Mark the project root directory as "Sources"
3. Right-click and run tests again

## Test Structure

```
test/
├── __init__.py
└── test_pricing_aggregation_etl.py
    ├── TestPricingAggregationETL          # Main functionality tests
    └── TestPricingAggregationETLConstants # Constants validation tests
```

## Test Coverage

The test suite covers:

- **Initialization**: ETL pipeline setup and configuration
- **Pre-validation**: Source file validation
- **Read Task**: Moving source files to transformation location
- **Main Task**: Data aggregation and transformations
- **Output Task**: Staging files to database tables (bronze and silver layers)
- **Constants**: Phase constants and variables

## Mocking

Tests use `unittest.mock` to mock external dependencies:

- PySpark DataFrames
- AWS S3 interactions
- Database connections
- LLM API calls
- File system operations

This ensures tests run quickly without requiring actual cloud resources.

## Adding New Tests

When adding new tests:

1. Create test methods in `test/test_pricing_aggregation_etl.py`
2. Follow the naming convention `test_*`
3. Use descriptive docstrings
4. Mock external dependencies
5. Use `setUp()` and `tearDown()` for test fixtures

Example:

```python
def test_new_feature(self):
    """Test description here"""
    # Arrange
    etl = PricingAggregationETL(...)

    # Act
    result = etl.some_method()

    # Assert
    self.assertEqual(result, expected_value)
```

## Continuous Integration

To run tests in CI/CD:

```yaml
# Example GitHub Actions workflow
- name: Run tests
  run: |
    pip install -e ".[dev]"
    pytest --cov=PricingAggregationETL --cov-report=xml
```

## Troubleshooting

### Import Errors

If you get `ModuleNotFoundError`:

1. Ensure the package is installed: `pip install -e .`
2. Verify you're in the correct Python environment
3. Check that `PYTHONPATH` includes the project root

### PySpark Errors

Tests mock PySpark, so you shouldn't see actual Spark errors. If you do:

1. Check that mocks are properly configured
2. Verify patch decorators are using correct paths
3. Ensure Java is installed for PySpark (if running integration tests)

### Missing Dependencies

Install dev dependencies:

```bash
pip install -e ".[dev]"
```
