"""Lotra application package.

Importing this package is intentionally side-effect free. It must not read
broker credentials, open browsers, make network requests, or import broker
adapters.
"""

__all__ = ["CLIENT_RELEASE", "RELEASE_DOWNLOAD_URL", "__version__"]

CLIENT_RELEASE = "lotra-v1.21.17"
RELEASE_DOWNLOAD_URL = "https://github.com/lotra-dev/lotra/releases/latest"
__version__ = "1.21.17"
