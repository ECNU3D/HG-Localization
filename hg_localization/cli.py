import click
from .config import default_config
from .dataset_manager import (
    download_dataset, 
    list_local_datasets, 
    list_s3_datasets,
    sync_local_dataset_to_s3 # Ensure this is imported if used by a command
)
from .model_manager import (
    download_model_metadata,
    list_local_models,
    list_s3_models,
    get_model_card_content,
    get_cached_model_card_content,
    get_cached_model_config_content,
    sync_local_model_to_s3,
    sync_all_local_models_to_s3
)
# config might be imported if CLI needs direct access to config values, but usually not.
# from .config import S3_BUCKET_NAME # Example, if needed

@click.group()
def cli():
    """CLI for Hugging Face dataset and model localization with S3 support."""
    pass

# --- Dataset Commands ---

@cli.command("download-dataset")
@click.argument('dataset_id')
@click.option('--name', '-n', default=None, help='The specific dataset configuration name (e.g., "mrpc" for glue, "en" for wikiann).')
@click.option('--revision', '-r', default=None, help='The git revision (branch, tag, commit hash) of the dataset.')
@click.option('--trust-remote-code', is_flag=True, help="Allow running code from the dataset's repository.")
@click.option('--make-public', is_flag=True, help="Zip and upload the dataset to a public S3 location with a public-read ACL, and update public_datasets.json.")
@click.option('--no-s3-upload', is_flag=True, help="Disable uploading the dataset to S3, only cache locally.")
def download_dataset_cmd(dataset_id: str, name: str | None, revision: str | None, trust_remote_code: bool, make_public: bool, no_s3_upload: bool):
    """Downloads dataset from Hugging Face, caches locally, and uploads to S3 if configured (unless --no-s3-upload is specified)."""
    click.echo(f"Processing dataset: {dataset_id} (Config: {name or 'default'}, Revision: {revision or 'default'}, Trust code: {trust_remote_code}, Make public: {make_public}, No S3 Upload: {no_s3_upload})...")
    success, message = download_dataset(
        dataset_id, 
        config_name=name, 
        revision=revision, 
        trust_remote_code=trust_remote_code,
        make_public=make_public,
        skip_s3_upload=no_s3_upload,
        config=default_config
    )
    if success:
        click.secho(f"Successfully processed '{dataset_id}'. Local path: {message}", fg="green")
        click.echo("Check S3 upload status in logs if S3 is configured.")
    else:
        click.secho(f"Failed to process '{dataset_id}'. Error: {message}", fg="red")

@cli.command("list-local-datasets")
def list_local_datasets_cmd():
    """Lists datasets available in the local cache."""
    click.echo("Listing local datasets from cache...")
    datasets = list_local_datasets(config=default_config)
    if not datasets:
        click.echo("No datasets found in local cache.")
        return
    click.secho("Available local datasets (cache):", bold=True)
    for ds_info in datasets:
        ds_id = ds_info.get('dataset_id', 'N/A')
        cfg_name = ds_info.get('config_name') or 'default' # Display 'default' if None
        rev = ds_info.get('revision') or 'default'      # Display 'default' if None
        click.echo(f"  - ID: {click.style(ds_id, fg='blue')}, Config: {click.style(cfg_name, fg='green')}, Revision: {click.style(rev, fg='yellow')}")

@cli.command("list-s3-datasets")
def list_s3_datasets_cmd():
    """Lists datasets available in the configured S3 bucket."""
    click.echo("Listing datasets from S3...")
    s3_datasets = list_s3_datasets(config=default_config)
    if not s3_datasets:
        click.echo("No datasets found in S3 or S3 not configured/accessible.")
        return

    click.echo(f"Found {len(s3_datasets)} dataset version(s) in S3:")
    for ds_info in s3_datasets:
        ds_id = ds_info.get('dataset_id', 'N/A')
        conf_name = ds_info.get('config_name') or "default"
        rev = ds_info.get('revision') or "default"
        s3_card_link = ds_info.get('s3_card_url')

        output = f"  - ID: {ds_id}, Config: {conf_name}, Revision: {rev}"
        if s3_card_link:
            output += f", Card (S3): {s3_card_link}"
        else:
            output += ", Card (S3): Not available"
        click.echo(output)

@cli.command("sync-local-dataset-to-s3")
@click.argument('dataset_id')
@click.option('--name', '-n', default=None, help='The specific dataset configuration name. Optional.')
@click.option('--revision', '-r', default=None, help='The git revision of the dataset. Optional.')
@click.option('--make-public', is_flag=True, help="Zip and upload the dataset to a public S3 location, and update public_datasets.json.")
def sync_local_dataset_to_s3_cmd(dataset_id: str, name: str | None, revision: str | None, make_public: bool):
    """Syncs a specific local dataset to S3. Uploads if not present; can also make public."""
    click.echo(f"Attempting to sync local dataset to S3: {dataset_id} (Config: {name or 'default'}, Revision: {revision or 'default'}, Make public: {make_public})")
    # Note: sync_local_dataset_to_s3 is now directly imported from dataset_manager
    success, message = sync_local_dataset_to_s3(
        dataset_id=dataset_id, 
        config_name=name, 
        revision=revision, 
        make_public=make_public,
        config=default_config
    )
    if success:
        click.secho(f"Successfully synced '{dataset_id}' (Config: {name or 'default'}, Revision: {revision or 'default'}). {message}", fg="green")
    else:
        click.secho(f"Failed to sync '{dataset_id}' (Config: {name or 'default'}, Revision: {revision or 'default'}). Error: {message}", fg="red")

# --- Model Commands ---

@cli.command("download-model")
@click.argument('model_id')
@click.option('--revision', '-r', default=None, help='The git revision (branch, tag, commit hash) of the model.')
@click.option('--make-public', is_flag=True, help="Upload the model to a public S3 location.")
@click.option('--no-s3-upload', is_flag=True, help="Disable uploading the model to S3, only cache locally.")
@click.option('--full-model', is_flag=True, help="Download the full model (weights, tokenizer, etc.) instead of just metadata.")
def download_model_cmd(model_id: str, revision: str | None, make_public: bool, no_s3_upload: bool, full_model: bool):
    """Downloads model metadata (card and config) or full model from Hugging Face, caches locally, and uploads to S3 if configured."""
    download_type = "full model" if full_model else "model metadata"
    click.echo(f"Processing {download_type}: {model_id} (Revision: {revision or 'default'}, Make public: {make_public}, No S3 Upload: {no_s3_upload})...")
    
    if full_model:
        click.echo(click.style("⚠️  WARNING: Full model download will download all model weights and may take a long time and use significant disk space.", fg="yellow"))
        if not click.confirm("Do you want to continue?"):
            click.echo("Download cancelled.")
            return
    
    success, message = download_model_metadata(
        model_id, 
        revision=revision, 
        make_public=make_public,
        skip_s3_upload=no_s3_upload,
        config=default_config,
        metadata_only=not full_model
    )
    if success:
        click.secho(f"Successfully processed {download_type} '{model_id}'. Local path: {message}", fg="green")
        click.echo("Check S3 upload status in logs if S3 is configured.")
    else:
        click.secho(f"Failed to process {download_type} '{model_id}'. Error: {message}", fg="red")

@cli.command("list-local-models")
def list_local_models_cmd():
    """Lists model metadata available in the local cache."""
    click.echo("Listing local model metadata from cache...")
    models = list_local_models(config=default_config)
    if not models:
        click.echo("No model metadata found in local cache.")
        return
    click.secho("Available local models (cache):", bold=True)
    for model_info in models:
        model_id = model_info.get('model_id', 'N/A')
        rev = model_info.get('revision') or 'default'
        has_card = 'Yes' if model_info.get('has_card') else 'No'
        has_config = 'Yes' if model_info.get('has_config') else 'No'
        model_type = "Full Model" if model_info.get('is_full_model', False) else "Metadata Only"
        type_color = 'green' if model_info.get('is_full_model', False) else 'cyan'
        click.echo(f"  - ID: {click.style(model_id, fg='blue')}, Revision: {click.style(rev, fg='yellow')}, Type: {click.style(model_type, fg=type_color)}, Card: {has_card}, Config: {has_config}")

@cli.command("list-s3-models")
def list_s3_models_cmd():
    """Lists models available in the configured S3 bucket."""
    click.echo("Listing models from S3...")
    s3_models = list_s3_models(config=default_config)
    if not s3_models:
        click.echo("No models found in S3 or S3 not configured/accessible.")
        return

    click.echo(f"Found {len(s3_models)} model version(s) in S3:")
    for model_info in s3_models:
        model_id = model_info.get('model_id', 'N/A')
        rev = model_info.get('revision') or "default"
        has_card = 'Yes' if model_info.get('has_card') else 'No'
        has_config = 'Yes' if model_info.get('has_config') else 'No'
        model_type = "Full Model" if model_info.get('is_full_model', False) else "Metadata Only"
        s3_card_url = model_info.get('s3_card_url')
        s3_config_url = model_info.get('s3_config_url')

        output = f"  - ID: {model_id}, Revision: {rev}, Type: {model_type}, Card: {has_card}, Config: {has_config}"
        if s3_card_url:
            output += f", Card URL: {s3_card_url}"
        if s3_config_url:
            output += f", Config URL: {s3_config_url}"
        click.echo(output)

@cli.command("show-model-card")
@click.argument('model_id')
@click.option('--revision', '-r', default=None, help='The git revision of the model.')
@click.option('--try-huggingface', is_flag=True, help="Try to fetch from Hugging Face if not found locally.")
def show_model_card_cmd(model_id: str, revision: str | None, try_huggingface: bool):
    """Shows the model card content for a cached model."""
    click.echo(f"Retrieving model card for: {model_id} (Revision: {revision or 'default'})")
    
    # First try cached content
    card_content = get_cached_model_card_content(model_id, revision=revision, config=default_config)
    
    # If not found and user wants to try HF
    if not card_content and try_huggingface:
        click.echo("Not found in cache, trying Hugging Face...")
        card_content = get_model_card_content(model_id, revision=revision)
    
    if card_content:
        click.echo("=" * 80)
        click.echo(card_content)
        click.echo("=" * 80)
    else:
        click.secho(f"Model card not found for '{model_id}' (Revision: {revision or 'default'})", fg="red")

@cli.command("show-model-config")
@click.argument('model_id')
@click.option('--revision', '-r', default=None, help='The git revision of the model.')
def show_model_config_cmd(model_id: str, revision: str | None):
    """Shows the model config.json content for a cached model."""
    click.echo(f"Retrieving model config for: {model_id} (Revision: {revision or 'default'})")
    
    config_content = get_cached_model_config_content(model_id, revision=revision, config=default_config)
    
    if config_content:
        click.echo("=" * 80)
        import json
        click.echo(json.dumps(config_content, indent=2))
        click.echo("=" * 80)
    else:
        click.secho(f"Model config not found for '{model_id}' (Revision: {revision or 'default'})", fg="red")

@cli.command("sync-local-model-to-s3")
@click.argument('model_id')
@click.option('--revision', '-r', default=None, help='The git revision of the model. Optional.')
@click.option('--make-public', is_flag=True, help='Make the model metadata public on S3 and update public_models.json.')
def sync_local_model_to_s3_cmd(model_id: str, revision: str | None, make_public: bool):
    """Syncs a specific local model to S3. Uploads if not present; can also make public."""
    click.echo(f"Syncing local model '{model_id}' (revision: {revision or 'default'}) to S3...")
    
    success, message = sync_local_model_to_s3(model_id, revision=revision, make_public=make_public, config=default_config)
    
    if success:
        click.secho(f"✓ {message}", fg="green")
    else:
        click.secho(f"✗ {message}", fg="red")

@cli.command("sync-all-local-models-to-s3")
@click.option('--make-public', is_flag=True, help='Make all model metadata public on S3 and update public_models.json.')
def sync_all_local_models_to_s3_cmd(make_public: bool):
    """Syncs all local models to S3. Uploads if not present; can also make public."""
    click.echo(f"Syncing all local models to S3 (make public: {make_public})...")
    
    sync_all_local_models_to_s3(make_public=make_public, config=default_config)

if __name__ == '__main__':
    cli() 