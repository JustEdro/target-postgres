# target-postgres

This is a [Singer](https://singer.io) target for Postgres
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

## Running and Configuration

Install the script by running e.g. `pip install -e .`. Then run the command like this:

```bash
<source> | target-postgres -c sample-config.json
```

Find an example of the configuration in the `sample-config.json` file.

## Tests

In order to run the tests install `pytest`:

```bash
pip install pytest
```

Then run the tests:

```bash
pytest
```