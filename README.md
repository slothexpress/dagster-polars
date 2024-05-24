# Python - Dagster - Polars
This project is a codetest with the aim to build a data pipeline using Python, Dagster and Polars. 

The pipeline should transform data and create assets that fulfil the following criteria:

1. Filter data on unique "First Name"
2. Anonymise data by shuffling "Last Name"
3. Group by job title and find mean Age

## Limitations
The pipeline does not persist the data/assets in any type of data warehouse or database.

## Dagster Quickstart


<div align="center">
  <a target="_blank" href="https://dagster.io" style="background:none">
    <img alt="dagster logo" src="https://github.com/dagster-io/dagster-quickstart/assets/5807118/7010804c-05a6-4ef4-bfc8-d9c88d458906" width="auto" height="120px">
  </a>
</div>


Get up-and-running with the Dagster quickstart project -- open the project in a GitHub Codespace and start building data pipelines with no local installation required.

For more information on how to use this project, please reference the [Dagster Quickstart guide](https://docs.dagster.io/getting-started/quickstart).

### Running The Project Locally

1. Clone the Dagster Quickstart repository:

    ```sh
    git clone https://github.com/dagster-io/dagster-quickstart

    cd dagster-quickstart
    ```

2. Install the required dependencies.

    Here we are using `-e`, for ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs), so that when Dagster code is modified, the changes automatically apply. 

    ```sh
    pip install -e ".[dev]"
    ```

3. Run the project!

    ```sh
    dagster dev
    ```

## Development

### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.
