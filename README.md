# Toloka-Prefect

This library makes the process of creating crowdsourcing pipelines easier by introducing out-of-the-box solutions for the majority of frequent steps in a typical [Toloka](https://toloka.ai/) pipeline. Here you can find most of [Toloka-Kit](https://github.com/Toloka/toloka-kit) functionality wrapped in [Prefect's](https://www.prefect.io/) interface.

Prefect is a widely-used workflow management system, designed for modern infrastructure and powered by the open-source Prefect Core workflow engine. It offers many options for workflow management (for more information on the internal logic refer to an official [example](https://github.com/Toloka/toloka-kit/blob/main/examples/9.toloka_and_ml_on_prefect/example.ipynb) of Toloka's usage with prefect - the functions here are basically organized in the same way).

To see how a basic pipeline built with this library might look, see the example in this repo.

You can find fully supported functions in the `toloka_prefect.py` file.

An overview of the library's function is in the file `functions_overview.md`. Almost every function has `token` (your Toloka requester account's OAuth token) and `env` (`'PRODUCTION'` or `'SANDBOX'`) arguments to connect to Toloka server. For more information on functions' arguments, return values and usage cases see docstrings.
