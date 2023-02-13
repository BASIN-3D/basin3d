from basin3d import monitor, synthesis
from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("basin3d")
except PackageNotFoundError:
    # package is not installed
    pass
__all__ = ['monitor', 'synthesis']