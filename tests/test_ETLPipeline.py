from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ETLPipeline import ETLPipeline


# Sample DataFrame for testing
@pytest.fixture
def sample_data():
    return pd.DataFrame({"column1": [1, 2, 3], "column2": ["a", "b", "c"]})


@pytest.fixture
def pipeline():
    return ETLPipeline()


# Test the extraction method
def test_extract(pipeline, sample_data):
    with patch("pandas.read_csv", return_value=sample_data):
        extracted_data = pipeline._extract("data/sample.csv")
        assert extracted_data.equals(sample_data)


# Test transformation method
def test_transform(pipeline):
    pipeline.con = MagicMock()
    transformation_config = {"duplicates_subset": ["column1"], "fillna_value": "Unknown", "filter_active": True}
    input_data = pd.DataFrame(
        {"column1": [1, 1, 2, None], "column2": ["a", "a", "b", "c"], "isActive": [True, True, False, True]}
    )
    output_data = pipeline._transform(input_data, transformation_config)
    assert len(output_data) == 2
    assert output_data["column1"].iloc[1] == "Unknown"
    assert output_data["isActive"].all()


# Test for the existence of a table
def test_table_exists(pipeline):
    pipeline.con = MagicMock()
    with patch.object(pipeline.con, "execute", side_effect=[None, Exception()]):
        assert pipeline._table_exists("existing_table")
        assert not pipeline._table_exists("non_existing_table")


# Test for loading data into an existing table
def test_load_existing_table(pipeline, sample_data):
    pipeline.con = MagicMock()
    with patch.object(pipeline, "_table_exists", return_value=True):
        pipeline._load(sample_data, "existing_table")
        pipeline.con.execute.assert_any_call("DELETE FROM existing_table")
        pipeline.con.execute.assert_any_call("INSERT INTO existing_table SELECT * FROM temp_existing_table")


# Test for loading data into a new table
def test_load_new_table(pipeline, sample_data):
    pipeline.con = MagicMock()
    with patch.object(pipeline, "_table_exists", return_value=False):
        pipeline._load(sample_data, "new_table")
        pipeline.con.execute.assert_called_with("CREATE TABLE new_table AS SELECT * FROM new_table")


# Test for the whole process
def test_process(pipeline, sample_data):
    pipeline.con = MagicMock()
    files = {
        "contacts": {
            "file_path": "data/contacts-20230414T185305.csv",
            "transformation_config": {
                "duplicates_subset": ["type", "company", "firstName"],
                "fillna_value": "Unknown",
                "filter_active": True,
            },
        },
        "products": {
            "file_path": "data/products-20230414T185305.csv",
            "transformation_config": {"duplicates_subset": ["name", "status"], "fillna_value": "Unknown"},
        },
        "purchase_orders": {
            "file_path": "data/purchase_orders-20230414T185305.csv",
            "transformation_config": {"duplicates_subset": None, "fillna_value": "Unknown"},
        },
        "sale_orders": {
            "file_path": "data/sale_order-20230414T185305.csv",
            "transformation_config": {"duplicates_subset": None, "fillna_value": "Unknown"},
        },
        "stockstream": {
            "file_path": "data/stockstream-20230414T185305.csv",
            "transformation_config": {"duplicates_subset": None, "fillna_value": "Unknown"},
        },
    }

    with patch.object(pipeline, "_extract", return_value=sample_data) as mock_extract, patch.object(
        pipeline, "_transform"
    ) as mock_transform, patch.object(pipeline, "_load") as mock_load:
        pipeline.process(files)
        assert mock_extract.call_count == 5
        assert mock_transform.call_count == 5
        assert mock_load.call_count == 5
