import pytest
from pathlib import Path
import zipfile
import os
import shutil
import tempfile

from hg_localization.utils import _get_safe_path_component, _restore_dataset_name, _zip_directory, _unzip_file

# --- Tests for _get_safe_path_component ---
@pytest.mark.parametrize("input_name, expected_output", [
    ("dataset/name", "dataset_name"),
    ("user\\dataset", "user_dataset"),
    ("config:name", "config_name"),
    ("file*name", "file_name"),
    ("query?name", "query_name"),
    ('"quoted"name', '_quoted_name'),
    ("<tag>name", "_tag_name"),
    (">arrow<name", "_arrow_name"),
    ("pipe|name", "pipe_name"),
    ("normal_name", "normal_name"),
    ("", ""),
    (None, ""),
    # ("all/slashes\\and:colons*q?\""'< >|', "all_slashes_and_colons_q_'___"),
])
def test_get_safe_path_component(input_name, expected_output):
    assert _get_safe_path_component(input_name) == expected_output

# --- Tests for _restore_dataset_name ---
@pytest.mark.parametrize("safe_name, expected_output", [
    ("dreamerdeo_finqa", "dreamerdeo/finqa"),
    ("microsoft_DialoGPT_medium", "microsoft_DialoGPT/medium"),
    ("huggingface_datasets", "huggingface/datasets"),
    ("simple_dataset", "simple/dataset"),
    ("my_data_set_realdataset", "my_data_set/realdataset"),
    ("nounderscores", "nounderscores"),
    ("", ""),
    (None, ""),
    ("single_word", "single/word"),
    ("org_name_dataset_name", "org_name_dataset/name"),
])
def test_restore_dataset_name(safe_name, expected_output):
    assert _restore_dataset_name(safe_name) == expected_output

# --- Tests for _zip_directory and _unzip_file ---

@pytest.fixture
def sample_directory_to_zip(tmp_path: Path) -> Path:
    source_dir = tmp_path / "source_to_zip"
    source_dir.mkdir()
    (source_dir / "file1.txt").write_text("content1")
    subdir = source_dir / "subdir"
    subdir.mkdir()
    (subdir / "file2.txt").write_text("content2")
    (source_dir / ".hiddenfile").write_text("hidden_content") # Test hidden files
    return source_dir

def test_zip_directory_success(sample_directory_to_zip: Path, tmp_path: Path):
    zip_path = tmp_path / "archive.zip"
    success = _zip_directory(sample_directory_to_zip, zip_path)
    assert success is True
    assert zip_path.exists()

    # Verify zip contents
    with zipfile.ZipFile(zip_path, 'r') as zf:
        names = set(zf.namelist())
        expected_names = {"file1.txt", "subdir/file2.txt", ".hiddenfile", "subdir/"}
        # For rglobbed items, paths are relative to the source_dir root
        assert names == expected_names 
        assert zf.read("file1.txt") == b"content1"
        assert zf.read("subdir/file2.txt") == b"content2"
        assert zf.read(".hiddenfile") == b"hidden_content"

def test_zip_directory_invalid_source(tmp_path: Path, capsys):
    non_existent_dir = tmp_path / "does_not_exist"
    zip_path = tmp_path / "archive.zip"
    success = _zip_directory(non_existent_dir, zip_path)
    assert success is False
    assert not zip_path.exists()
    captured = capsys.readouterr()
    assert f"Error: {non_existent_dir} is not a valid directory to zip." in captured.out

def test_unzip_file_success(sample_directory_to_zip: Path, tmp_path: Path):
    zip_path = tmp_path / "archive_to_unzip.zip"
    assert _zip_directory(sample_directory_to_zip, zip_path) # Create the zip first

    extract_to_dir = tmp_path / "extracted_content"
    success = _unzip_file(zip_path, extract_to_dir)
    assert success is True
    assert extract_to_dir.exists()

    # Verify extracted contents mirror the original sample_directory_to_zip structure
    assert (extract_to_dir / "file1.txt").read_text() == "content1"
    assert (extract_to_dir / "subdir" / "file2.txt").read_text() == "content2"
    assert (extract_to_dir / ".hiddenfile").read_text() == "hidden_content"

def test_unzip_file_invalid_zip(tmp_path: Path, capsys):
    non_zip_file = tmp_path / "not_a_zip.txt"
    non_zip_file.write_text("this is not a zip file")
    extract_to_dir = tmp_path / "extracted_fail"
    success = _unzip_file(non_zip_file, extract_to_dir)
    assert success is False
    assert extract_to_dir.exists() # Directory is created before attempting unzip
    captured = capsys.readouterr()
    assert f"Error unzipping file {non_zip_file}" in captured.out

def test_unzip_file_corrupted_zip(tmp_path: Path, capsys):
    corrupted_zip_path = tmp_path / "corrupted.zip"
    with open(corrupted_zip_path, 'wb') as f:
        f.write(b"PK\x03\x04\x0a\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00This is not a valid zip ending.")
    
    extract_to_dir = tmp_path / "extracted_corrupt_fail"
    success = _unzip_file(corrupted_zip_path, extract_to_dir)
    assert success is False
    # extract_to_dir might be created by os.makedirs, but content extraction should fail.
    # Check specific error message if needed, e.g., relating to zipfile.BadZipFile
    captured = capsys.readouterr()
    assert f"Error unzipping file {corrupted_zip_path}" in captured.out 