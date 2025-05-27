import click
from .config import default_config
from .dataset_manager import (
    download_dataset, 
    list_local_datasets, 
    list_s3_datasets,
    sync_local_dataset_to_s3 # Ensure this is imported if used by a command
)
# config might be imported if CLI needs direct access to config values, but usually not.
# from .config import S3_BUCKET_NAME # Example, if needed

@click.group()
def cli():
    """CLI for Hugging Face dataset localization with S3 support."""
    pass

@cli.command()
@click.argument('dataset_id')
@click.option('--name', '-n', default=None, help='The specific dataset configuration name (e.g., "mrpc" for glue, "en" for wikiann).')
@click.option('--revision', '-r', default=None, help='The git revision (branch, tag, commit hash) of the dataset.')
@click.option('--trust-remote-code', is_flag=True, help="Allow running code from the dataset's repository.")
@click.option('--make-public', is_flag=True, help="Zip and upload the dataset to a public S3 location with a public-read ACL, and update public_datasets.json.")
@click.option('--no-s3-upload', is_flag=True, help="Disable uploading the dataset to S3, only cache locally.")
def download(dataset_id: str, name: str | None, revision: str | None, trust_remote_code: bool, make_public: bool, no_s3_upload: bool):
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

@cli.command("list-local")
def list_local_cmd():
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

@cli.command("list-s3")
def list_s3_command():
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

@cli.command("sync-local-to-s3")
@click.argument('dataset_id')
@click.option('--name', '-n', default=None, help='The specific dataset configuration name. Optional.')
@click.option('--revision', '-r', default=None, help='The git revision of the dataset. Optional.')
@click.option('--make-public', is_flag=True, help="Zip and upload the dataset to a public S3 location, and update public_datasets.json.")
def sync_local_to_s3_cmd(dataset_id: str, name: str | None, revision: str | None, make_public: bool):
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

if __name__ == '__main__':
    cli() 