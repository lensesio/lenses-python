# Python - Lenses for Apache Kafka

This is a python library that implements the [Lenses](http://www.landoop.com/kafka-lenses) REST and WS APIs.

[![build status](https://img.shields.io/travis/Landoop/lenses-python/dev.svg?style=flat-square)](https://travis-ci.org/Landoop/lenses-python)

# Documentation

See [Lenses Python documentation](https://lenses.stream/dev/python-lib/).

# Installation

```bash
pip3 install lenses_python
```

# Use Cases and Examples

* CI/CD and Automation
* Jupyter Notebooks
* Machine Learning

## Jupyter Example

<p align="center">
  <img src="https://pbs.twimg.com/media/DbeXsAZXcAAw8uy.jpg" width="400"/>
</p>

## Integration Tests

Run tests:

```
make LICENSE_KEY=<YOUR-LICENSE-KEY> docker
make test
```

This command will start a Lenses docker box and when it's ready the integrationt ests will run.

*Note*: You can find a dev license key [here](https://www.landoop.com/downloads/)

## License

The project is licensed under the Apache 2 license.
