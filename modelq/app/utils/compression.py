"""
Compression utilities for base64 data.
Provides functions to compress and decompress base64-encoded data.
Supports multiple compression algorithms: zlib, gzip, bz2, brotli, lz4
"""
import base64
import zlib
import gzip
import bz2
from typing import Optional, Literal

# Try to import optional compression libraries
try:
    import brotli
    BROTLI_AVAILABLE = True
except ImportError:
    BROTLI_AVAILABLE = False

try:
    import lz4.frame
    LZ4_AVAILABLE = True
except ImportError:
    LZ4_AVAILABLE = False

CompressionMethod = Literal["zlib", "gzip", "bz2", "brotli", "lz4"]


def compress_base64(
    base64_string: str,
    compression_level: int = 6,
    method: CompressionMethod = "zlib"
) -> str:
    """
    Compress a base64 string using various compression algorithms.

    Args:
        base64_string: The base64 encoded string to compress
        compression_level: Compression level (0-9 for most methods). Default is 6.
                          - zlib/gzip: 0-9 (0=none, 6=balanced, 9=max)
                          - bz2: 1-9 (1=fastest, 9=best compression)
                          - brotli: 0-11 (0=fastest, 11=best compression)
                          - lz4: level ignored (always fast)
        method: Compression method to use. Options:
                - "zlib": Fast and balanced (default)
                - "gzip": Standard, similar to zlib
                - "bz2": Better compression, slower
                - "brotli": Best compression, moderate speed (requires brotli package)
                - "lz4": Fastest, lower compression (requires lz4 package)

    Returns:
        Compressed base64 string with method prefix (e.g., "zlib:...")
    """
    if not base64_string:
        return base64_string

    try:
        # Encode string to bytes
        data_bytes = base64_string.encode('utf-8')

        # Compress based on selected method
        if method == "zlib":
            compressed_bytes = zlib.compress(data_bytes, level=compression_level)

        elif method == "gzip":
            compressed_bytes = gzip.compress(data_bytes, compresslevel=compression_level)

        elif method == "bz2":
            # bz2 uses compresslevel 1-9
            level = max(1, min(9, compression_level))
            compressed_bytes = bz2.compress(data_bytes, compresslevel=level)

        elif method == "brotli":
            if not BROTLI_AVAILABLE:
                raise ImportError("brotli package not installed. Install with: pip install brotli")
            # brotli uses quality 0-11
            quality = max(0, min(11, compression_level))
            compressed_bytes = brotli.compress(data_bytes, quality=quality)

        elif method == "lz4":
            if not LZ4_AVAILABLE:
                raise ImportError("lz4 package not installed. Install with: pip install lz4")
            compressed_bytes = lz4.frame.compress(data_bytes)

        else:
            raise ValueError(f"Unknown compression method: {method}")

        # Encode to base64 with method prefix
        compressed_base64 = base64.b64encode(compressed_bytes).decode('utf-8')

        # Add method prefix so we know how to decompress
        return f"{method}:{compressed_base64}"

    except Exception as e:
        # If compression fails, return original string
        return base64_string


def decompress_base64(compressed_base64: str) -> Optional[str]:
    """
    Decompress a compressed base64 string.
    Automatically detects compression method from prefix.

    Args:
        compressed_base64: The compressed base64 string (with method prefix)

    Returns:
        Decompressed base64 string, or None if decompression fails
    """
    if not compressed_base64:
        return compressed_base64

    try:
        # Check if there's a method prefix
        if ":" in compressed_base64:
            method, compressed_data = compressed_base64.split(":", 1)
        else:
            # Backward compatibility: assume zlib if no prefix
            method = "zlib"
            compressed_data = compressed_base64

        # Decode from base64 to compressed bytes
        compressed_bytes = base64.b64decode(compressed_data)

        # Decompress based on method
        if method == "zlib":
            decompressed_bytes = zlib.decompress(compressed_bytes)

        elif method == "gzip":
            decompressed_bytes = gzip.decompress(compressed_bytes)

        elif method == "bz2":
            decompressed_bytes = bz2.decompress(compressed_bytes)

        elif method == "brotli":
            if not BROTLI_AVAILABLE:
                raise ImportError("brotli package not installed")
            decompressed_bytes = brotli.decompress(compressed_bytes)

        elif method == "lz4":
            if not LZ4_AVAILABLE:
                raise ImportError("lz4 package not installed")
            decompressed_bytes = lz4.frame.decompress(compressed_bytes)

        else:
            raise ValueError(f"Unknown compression method: {method}")

        # Decode back to string
        decompressed_string = decompressed_bytes.decode('utf-8')

        return decompressed_string

    except Exception as e:
        # If decompression fails, assume it's not compressed and return as-is
        return compressed_base64


def is_compressed(data: str) -> bool:
    """
    Check if a base64 string is compressed.

    Args:
        data: The base64 string to check

    Returns:
        True if the data appears to be compressed, False otherwise
    """
    if not data:
        return False

    try:
        # Try to decode and decompress
        compressed_bytes = base64.b64decode(data)
        zlib.decompress(compressed_bytes)
        return True
    except:
        return False
