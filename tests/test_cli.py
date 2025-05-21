import pytest
from click.testing import CliRunner
from hg_localization.cli import cli
from hg_localization.core import DEFAULT_CONFIG_NAME, DEFAULT_REVISION_NAME # For checking default outputs

# Mock the core functions that the CLI calls
# We want to test the CLI logic (parsing args, calling core functions, formatting output)
# not the core functions themselves (those are tested in test_core.py)

@pytest.fixture
def mock_core_functions_for_cli(mocker):
    """Mocks core functions used by the CLI, targeting their location in cli.py's import scope."""
    mock_download = mocker.patch('hg_localization.cli.download_dataset')
    mock_list_local = mocker.patch('hg_localization.cli.list_local_datasets')
    mock_list_s3 = mocker.patch('hg_localization.cli.list_s3_datasets')
    return {
        "download_dataset": mock_download,
        "list_local_datasets": mock_list_local,
        "list_s3_datasets": mock_list_s3
    }

@pytest.fixture
def s3_dataset_list_with_cards():
    """Provides a sample list of S3 dataset info including s3_card_url."""
    return [
        {
            "dataset_id": "s3_d1", 
            "config_name": "s3_c1", 
            "revision": "s3_r1", 
            "s3_card_url": "https://s3.example.com/card_d1.md?presigned"
        },
        {
            "dataset_id": "s3_d2", 
            "config_name": None, # Default config
            "revision": "s3_r2", 
            "s3_card_url": None # Card not available
        },
        {
            "dataset_id": "s3_d3", 
            "config_name": "s3_c3", 
            "revision": None, # Default revision
            "s3_card_url": "https://s3.example.com/card_d3.md?presigned"
        }
    ]

def test_cli_download_full_spec(mock_core_functions_for_cli):
    runner = CliRunner()
    mock_download = mock_core_functions_for_cli['download_dataset']
    fake_path = "/fake/path/my_dataset/my_config/my_revision"
    mock_download.return_value = (True, fake_path)

    result = runner.invoke(cli, [
        'download', 'my_dataset', 
        '--name', 'my_config', 
        '--revision', 'my_revision', 
        '--trust-remote-code'
    ])

    assert result.exit_code == 0
    mock_download.assert_called_once_with(
        'my_dataset', 
        config_name='my_config', 
        revision='my_revision', 
        trust_remote_code=True,
        make_public=False,
        skip_s3_upload=False
    )
    assert f"Successfully processed 'my_dataset'. Local path: {fake_path}" in result.output

def test_cli_download_id_only(mock_core_functions_for_cli):
    runner = CliRunner()
    mock_download = mock_core_functions_for_cli['download_dataset']
    fake_path = f"/fake/path/id_only_dataset/{DEFAULT_CONFIG_NAME}/{DEFAULT_REVISION_NAME}"
    mock_download.return_value = (True, fake_path)

    result = runner.invoke(cli, ['download', 'id_only_dataset'])

    assert result.exit_code == 0
    mock_download.assert_called_once_with(
        'id_only_dataset', 
        config_name=None,  # CLI default for --name is None
        revision=None,     # CLI default for --revision is None
        trust_remote_code=False,
        make_public=False,
        skip_s3_upload=False
    )
    assert f"Successfully processed 'id_only_dataset'. Local path: {fake_path}" in result.output

def test_cli_download_failure(mock_core_functions_for_cli):
    runner = CliRunner()
    mock_download = mock_core_functions_for_cli['download_dataset']
    mock_download.return_value = (False, "Epic fail downloading")

    result = runner.invoke(cli, ['download', 'bad_dataset', '-n', 'bad_config'])

    assert result.exit_code == 0 
    mock_download.assert_called_once_with('bad_dataset', config_name='bad_config', revision=None, trust_remote_code=False, make_public=False, skip_s3_upload=False)
    assert "Failed to process 'bad_dataset'. Error: Epic fail downloading" in result.output

def test_cli_list_local_empty(mock_core_functions_for_cli):
    runner = CliRunner()
    mock_list_local = mock_core_functions_for_cli['list_local_datasets']
    mock_list_local.return_value = []

    result = runner.invoke(cli, ['list-local'])
    assert result.exit_code == 0
    mock_list_local.assert_called_once()
    assert "No datasets found in local cache." in result.output

def test_cli_list_local_with_data(mock_core_functions_for_cli):
    runner = CliRunner()
    mock_list_local = mock_core_functions_for_cli['list_local_datasets']
    mock_list_local.return_value = [
        {"dataset_id": "d1", "config_name": "c1", "revision": "r1"},
        {"dataset_id": "d2", "config_name": None, "revision": "r2"}, # Default config
        {"dataset_id": "d3", "config_name": "c3", "revision": None}  # Default revision
    ]
    result = runner.invoke(cli, ['list-local'])
    assert result.exit_code == 0
    assert "ID: d1, Config: c1, Revision: r1" in result.output
    assert "ID: d2, Config: default, Revision: r2" in result.output # CLI shows 'default' for None
    assert "ID: d3, Config: c3, Revision: default" in result.output # CLI shows 'default' for None

def test_cli_list_s3_empty(mock_core_functions_for_cli):
    runner = CliRunner()
    mock_list_s3 = mock_core_functions_for_cli['list_s3_datasets']
    mock_list_s3.return_value = []

    result = runner.invoke(cli, ['list-s3'])
    assert result.exit_code == 0
    mock_list_s3.assert_called_once()
    assert "No datasets found in S3 or S3 not configured/accessible." in result.output

def test_cli_list_s3_with_data(mock_core_functions_for_cli):
    runner = CliRunner()
    mock_list_s3 = mock_core_functions_for_cli['list_s3_datasets']
    mock_list_s3.return_value = [
        {"dataset_id": "s3_d1", "config_name": "s3_c1", "revision": "s3_r1"},
        {"dataset_id": "s3_d2", "config_name": None, "revision": None} 
    ]
    result = runner.invoke(cli, ['list-s3'])
    assert result.exit_code == 0
    assert "ID: s3_d1, Config: s3_c1, Revision: s3_r1" in result.output
    assert "ID: s3_d2, Config: default, Revision: default" in result.output

def test_cli_download_missing_dataset_id(mock_core_functions_for_cli):
    runner = CliRunner()
    result = runner.invoke(cli, ['download'])
    assert result.exit_code != 0 
    assert "Missing argument 'DATASET_ID'" in result.output
    mock_core_functions_for_cli["download_dataset"].assert_not_called() 

def test_cli_list_s3_with_datasets(mock_core_functions_for_cli, s3_dataset_list_with_cards):
    runner = CliRunner()
    mock_list_s3 = mock_core_functions_for_cli['list_s3_datasets']
    mock_list_s3.return_value = s3_dataset_list_with_cards

    result = runner.invoke(cli, ['list-s3'])
    assert result.exit_code == 0
    mock_list_s3.assert_called_once()
    
    # Adjust assertions to match the new fixture data
    assert "ID: s3_d1, Config: s3_c1, Revision: s3_r1, Card (S3): https://s3.example.com/card_d1.md?presigned" in result.output
    assert "ID: s3_d2, Config: default, Revision: s3_r2, Card (S3): Not available" in result.output
    assert "ID: s3_d3, Config: s3_c3, Revision: default, Card (S3): https://s3.example.com/card_d3.md?presigned" in result.output 