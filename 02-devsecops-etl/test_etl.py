import sqlite3
import pytest
from unittest.mock import MagicMock, patch
from etl import init_db, extract_data, transform_data, load_data

@pytest.fixture
def temp_db(tmp_path):
    """Fixture to create a temporary database path."""
    return str(tmp_path / "test_weather.db")

def test_init_db(temp_db):
    """Test that database and table are correctly created."""
    init_db(temp_db)
    conn = sqlite3.connect(temp_db)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='weather_records'")
    table_exists = cursor.fetchone()
    assert table_exists is not None
    conn.close()

def test_transform_data_valid():
    """Test data transform with a valid payload."""
    payload = {
        "latitude": 50.8503,
        "longitude": 4.3517,
        "current_weather": {
            "time": "2026-06-17T12:00:00Z",
            "temperature": 18.5,
            "windspeed": 12.3,
            "weathercode": 3
        }
    }
    result = transform_data(payload)
    assert result["timestamp"] == "2026-06-17T12:00:00Z"
    assert result["temperature"] == 18.5
    assert result["windspeed"] == 12.3
    assert result["weathercode"] == 3
    assert "ingested_at" in result

def test_transform_data_invalid():
    """Test that data transform raises ValueError on invalid payload schema."""
    invalid_payload = {
        "latitude": 50.8503,
        "current_weather": {
            # missing required field windspeed & weathercode
            "time": "2026-06-17T12:00:00Z",
            "temperature": 18.5
        }
    }
    with pytest.raises(ValueError, match="Invalid weather payload structure"):
        transform_data(invalid_payload)

def test_load_data(temp_db):
    """Test loading data into database and verifying persistence."""
    init_db(temp_db)
    data = {
        "timestamp": "2026-06-17T12:00:00Z",
        "temperature": 18.5,
        "windspeed": 12.3,
        "weathercode": 3,
        "ingested_at": "2026-06-17T12:00:05Z"
    }
    load_data(temp_db, data)
    
    # Verify DB content
    conn = sqlite3.connect(temp_db)
    cursor = conn.cursor()
    cursor.execute("SELECT timestamp, temperature, windspeed, weathercode FROM weather_records")
    row = cursor.fetchone()
    assert row is not None
    assert row[0] == "2026-06-17T12:00:00Z"
    assert row[1] == 18.5
    assert row[2] == 12.3
    assert row[3] == 3
    conn.close()

@patch("httpx.Client.get")
def test_extract_data(mock_get):
    """Test extracting data mocks the httpx request correctly."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"status": "ok"}
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response
    
    res = extract_data("https://api.test", "1.0", "2.0")
    assert res == {"status": "ok"}
    mock_get.assert_called_once_with("https://api.test", params={
        "latitude": "1.0",
        "longitude": "2.0",
        "current_weather": "true"
    })
