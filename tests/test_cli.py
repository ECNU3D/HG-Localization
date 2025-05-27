import pytest
from click.testing import CliRunner
from unittest.mock import patch, MagicMock, ANY

# Assuming cli.py is in hg_localization folder, and this test is in tests/
# Adjust import path if structure is different
from hg_localization.cli import cli 

# We need to mock the functions called by the CLI commands.
# These functions are now in dataset_manager.py

@pytest.fixture
def mock_dataset_manager_functions(mocker):
    """Mocks all relevant functions from dataset_manager.py used by cli.py."""
    mock_download = mocker.patch('hg_localization.cli.download_dataset')
    mock_list_local = mocker.patch('hg_localization.cli.list_local_datasets')
    mock_list_s3 = mocker.patch('hg_localization.cli.list_s3_datasets')
    mock_sync_local_to_s3 = mocker.patch('hg_localization.cli.sync_local_dataset_to_s3')
    return {
        "download_dataset": mock_download,
        "list_local_datasets": mock_list_local,
        "list_s3_datasets": mock_list_s3,
        "sync_local_dataset_to_s3": mock_sync_local_to_s3
    }

def test_download_command_success(mock_dataset_manager_functions):
    runner = CliRunner()
    mock_dataset_manager_functions["download_dataset"].return_value = (True, "/fake/path/to/dataset")
    
    result = runner.invoke(cli, ["download", "test_dataset", "--name", "config1", "-r", "rev1", "--trust-remote-code", "--no-s3-upload"])
    
    assert result.exit_code == 0
    assert "Successfully processed 'test_dataset'" in result.output
    assert "Local path: /fake/path/to/dataset" in result.output
    mock_dataset_manager_functions["download_dataset"].assert_called_once_with(
        "test_dataset", 
        config_name="config1", 
        revision="rev1", 
        trust_remote_code=True,
        make_public=False, # Default for make_public flag
        skip_s3_upload=True,
        config=ANY  # The CLI now passes the default_config
    )

def test_download_command_failure(mock_dataset_manager_functions):
    runner = CliRunner()
    mock_dataset_manager_functions["download_dataset"].return_value = (False, "Mocked download error")
    result = runner.invoke(cli, ["download", "test_dataset_fail"])
    assert result.exit_code == 0 # Click commands usually exit 0 even on app-level failure, relying on output
    assert "Failed to process 'test_dataset_fail'" in result.output
    assert "Error: Mocked download error" in result.output

def test_list_local_command_empty(mock_dataset_manager_functions):
    runner = CliRunner()
    mock_dataset_manager_functions["list_local_datasets"].return_value = []
    result = runner.invoke(cli, ["list-local"])
    assert result.exit_code == 0
    assert "No datasets found in local cache." in result.output

def test_list_local_command_with_data(mock_dataset_manager_functions):
    runner = CliRunner()
    mock_data = [
        {"dataset_id": "ds1", "config_name": "cfgA", "revision": "revB"},
        {"dataset_id": "ds2", "config_name": None, "revision": None} # Test None for default display
    ]
    mock_dataset_manager_functions["list_local_datasets"].return_value = mock_data
    result = runner.invoke(cli, ["list-local"])
    assert result.exit_code == 0
    assert "Available local datasets (cache):" in result.output
    assert "ID: ds1, Config: cfgA, Revision: revB" in result.output
    assert "ID: ds2, Config: default, Revision: default" in result.output # Check default display
    mock_dataset_manager_functions["list_local_datasets"].assert_called_once_with(config=ANY)

def test_list_s3_command_empty(mock_dataset_manager_functions):
    runner = CliRunner()
    mock_dataset_manager_functions["list_s3_datasets"].return_value = []
    result = runner.invoke(cli, ["list-s3"])
    assert result.exit_code == 0
    assert "No datasets found in S3 or S3 not configured/accessible." in result.output

def test_list_s3_command_with_data(mock_dataset_manager_functions):
    runner = CliRunner()
    mock_data = [
        {"dataset_id": "s3_ds1", "config_name": "s3_cfgA", "revision": "s3_revB", "s3_card_url": "http://s3_card_link_1"},
        {"dataset_id": "s3_ds2", "config_name": None, "revision": "s3_revC", "s3_card_url": None}
    ]
    mock_dataset_manager_functions["list_s3_datasets"].return_value = mock_data
    result = runner.invoke(cli, ["list-s3"])
    assert result.exit_code == 0
    assert f"Found {len(mock_data)} dataset version(s) in S3:" in result.output
    assert "ID: s3_ds1, Config: s3_cfgA, Revision: s3_revB, Card (S3): http://s3_card_link_1" in result.output
    assert "ID: s3_ds2, Config: default, Revision: s3_revC, Card (S3): Not available" in result.output
    mock_dataset_manager_functions["list_s3_datasets"].assert_called_once_with(config=ANY)

def test_sync_local_to_s3_command_success(mock_dataset_manager_functions):
    runner = CliRunner()
    mock_dataset_manager_functions["sync_local_dataset_to_s3"].return_value = (True, "Sync successful mock message")
    result = runner.invoke(cli, ["sync-local-to-s3", "my_dataset_to_sync", "--name", "specific_config", "--make-public"])
    
    assert result.exit_code == 0
    assert "Successfully synced 'my_dataset_to_sync'" in result.output
    assert "Sync successful mock message" in result.output
    mock_dataset_manager_functions["sync_local_dataset_to_s3"].assert_called_once_with(
        dataset_id="my_dataset_to_sync",
        config_name="specific_config",
        revision=None, # Default if not provided
        make_public=True,
        config=ANY  # The CLI now passes the default_config
    )

def test_sync_local_to_s3_command_failure(mock_dataset_manager_functions):
    runner = CliRunner()
    mock_dataset_manager_functions["sync_local_dataset_to_s3"].return_value = (False, "Mocked sync error")
    result = runner.invoke(cli, ["sync-local-to-s3", "failed_sync_ds"])
    
    assert result.exit_code == 0 # Click command itself succeeds
    assert "Failed to sync 'failed_sync_ds'" in result.output
    assert "Error: Mocked sync error" in result.output 