import os
from pathlib import Path
from datetime import datetime
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from airflow import DAG

DEFAULT_DBT_ROOT_PATH = Path(__file__).parent.parent / "dags" / "dbt_sample_project"
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))
profile_config = ProfileConfig(
     profile_name="dbt_sample_project",
     target_name="fabric-dev",
     profiles_yml_filepath=DBT_ROOT_PATH / "profiles.yml",
)

dbt_fabric_dag = DbtDag(
     project_config=ProjectConfig(DBT_ROOT_PATH,),
     operator_args={"install_deps": True},
     profile_config=profile_config,
     schedule_interval="@daily",
     start_date=datetime(2025, 7, 31),
     catchup=False,
     dag_id="dbt_fabric_dag",
)