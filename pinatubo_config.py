"""Shared, dependency-free configuration and external output path validation."""
from pathlib import Path
import json
import os

REPO_ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT = '~/DATA/Pinatubo1991Processing'


def read_config(path):
    path = Path(path).expanduser().resolve()
    with path.open(encoding='utf-8') as stream:
        config = json.load(stream)
    if not isinstance(config, dict):
        raise ValueError(f'Expected a JSON object in {path}')
    return config


def resolve_path(value, *, relative_to):
    if not isinstance(value, (str, os.PathLike)) or not str(value).strip():
        raise ValueError('A nonempty path is required')
    expanded = os.path.expandvars(os.path.expanduser(str(value)))
    if '$' in expanded:
        raise ValueError(f'Unresolved environment variable in path: {value}')
    path = Path(expanded)
    return (path if path.is_absolute() else Path(relative_to) / path).resolve()


def external_output_root(value, *, relative_to, repo_root=None):
    """Validate without creating anything; resolve symlinks before containment checks."""
    path = resolve_path(value, relative_to=relative_to)
    repo = Path(repo_root).resolve() if repo_root is not None else REPO_ROOT
    if path == repo or repo in path.parents:
        raise ValueError(f'Output must be outside the repository: {path}')
    for parent in (path, *path.parents):
        if (parent / '.git').exists():
            raise ValueError(f'Output is inside a Git checkout: {path}')
    return path


def analysis_products_root(config, analysis_dir):
    value = os.environ.get('PINATUBO_ANALYSIS_OUTPUT') or config.get('output_root')
    value = value or f'{DEFAULT_OUTPUT}/analysis'
    return external_output_root(value, relative_to=analysis_dir)


def analysis_network_products(config, analysis_dir, products_root):
    value = config.get('network_history_products')
    if value:
        return resolve_path(value, relative_to=analysis_dir)
    return Path(products_root) / 'network_history'
