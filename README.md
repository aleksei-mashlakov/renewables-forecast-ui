# renewables-forecast-ui

[![Release](https://img.shields.io/github/v/release/aleksei-mashlakov/renewables-forecast-ui)](https://img.shields.io/github/v/release/aleksei-mashlakov/renewables-forecast-ui)
[![Build status](https://img.shields.io/github/actions/workflow/status/aleksei-mashlakov/renewables-forecast-ui/main.yml?branch=main)](https://github.com/aleksei-mashlakov/renewables-forecast-ui/actions/workflows/main.yml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/aleksei-mashlakov/renewables-forecast-ui/branch/main/graph/badge.svg)](https://codecov.io/gh/aleksei-mashlakov/renewables-forecast-ui)
[![Commit activity](https://img.shields.io/github/commit-activity/m/aleksei-mashlakov/renewables-forecast-ui)](https://img.shields.io/github/commit-activity/m/aleksei-mashlakov/renewables-forecast-ui)
[![License](https://img.shields.io/github/license/aleksei-mashlakov/renewables-forecast-ui)](https://img.shields.io/github/license/aleksei-mashlakov/renewables-forecast-ui)

A repository to host [renewables forecast page](https://aleksei-mashlakov.github.io/renewables-forecast-ui/).

- **Github repository**: <https://github.com/aleksei-mashlakov/renewables-forecast-ui/>
- **Documentation** <https://aleksei-mashlakov.github.io/renewables-forecast-ui/>

## Develop

### Set Up Your Development Environment

Then, install the environment and the pre-commit hooks with

```bash
make install
```

This will also generate your `uv.lock` file

### Run the pre-commit hooks

Initially, the CI/CD pipeline might be failing due to formatting issues. To resolve those run:

```bash
uv run pre-commit run -a
```
---

Repository initiated with [fpgmaas/cookiecutter-uv](https://github.com/fpgmaas/cookiecutter-uv).
