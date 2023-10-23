# dagster-data-diff-demo
Dagster + Datafold: Better Together

This is a demo git repo for the Dagster + data-diff integration. It's goal is to give you clear examples of how to use Dagster's asset checks to solve for replication problems in your data pipelines.

## Setup

```bash

# setup python dependencies
python -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
source venv/bin/activate
```

```bash
# setup dagster
cd data-diff-demo

# start dagster development server
dagster dev
```

TODO: add GIF on materializing all assets and clicking through UI

