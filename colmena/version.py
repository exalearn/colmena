# we target 3.8+, so this should be okay without fallback to importlib_metadata
import importlib.metadata

# single source of truth for package version,
# see https://packaging.python.org/en/latest/single_source_version/

__version__ = importlib.metadata.version('colmena')
