"""
Testes simples para validar estrutura dos scripts Glue.

Estes testes não executam Spark.
Eles garantem que os arquivos esperados existem e que os parâmetros principais estão presentes.
"""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"


def test_glue_scripts_exist():
    expected = [
        "raw_to_bronze.py",
        "bronze_to_silver.py",
        "silver_to_gold.py",
    ]
    for script in expected:
        assert (SCRIPTS / script).exists(), f"Script ausente: {script}"


def test_required_glue_parameters_are_declared():
    required_params = ["JOB_NAME", "DATA_LAKE_BUCKET", "DATA_DATE", "ENVIRONMENT"]
    for script in SCRIPTS.glob("*.py"):
        content = script.read_text(encoding="utf-8")
        for param in required_params:
            assert param in content, f"{param} não encontrado em {script.name}"


def test_scripts_use_job_commit():
    for script in SCRIPTS.glob("*.py"):
        content = script.read_text(encoding="utf-8")
        assert "job.commit()" in content, f"job.commit() ausente em {script.name}"
