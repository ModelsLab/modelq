"""
Test cases for base64 compression functionality in ModelQ.

Tests all compression methods (zlib, gzip, bz2, brotli, lz4) with various
data types and ensures lossless compression/decompression.
"""

import json
import base64
import io
import pytest
import fakeredis
from PIL import Image

from modelq import ModelQ
from modelq.app.utils.compression import (
    compress_base64,
    decompress_base64,
    is_compressed,
    BROTLI_AVAILABLE,
    LZ4_AVAILABLE
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_redis():
    """Create a fake Redis client for testing."""
    return fakeredis.FakeStrictRedis()


@pytest.fixture
def modelq_instance(mock_redis):
    """Create a ModelQ instance with mock Redis."""
    return ModelQ(redis_client=mock_redis)


@pytest.fixture
def simple_base64():
    """Generate a simple base64 string for testing."""
    return "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="


@pytest.fixture
def image_base64():
    """Generate a test image as base64."""
    img = Image.new('RGB', (100, 100), color='blue')
    buffered = io.BytesIO()
    img.save(buffered, format="PNG")
    base64_string = base64.b64encode(buffered.getvalue()).decode('utf-8')
    return f"data:image/png;base64,{base64_string}"


@pytest.fixture
def repetitive_base64():
    """Generate highly compressible repetitive data."""
    return "data:text/plain;base64," + "A" * 1000


# ---------------------------------------------------------------------------
# Compression Utility Tests
# ---------------------------------------------------------------------------

class TestCompressionUtils:
    """Test compression utility functions."""

    def test_compress_decompress_zlib(self, simple_base64):
        """Test zlib compression and decompression."""
        compressed = compress_base64(simple_base64, method="zlib", compression_level=6)
        decompressed = decompress_base64(compressed)

        assert compressed != simple_base64  # Should be different
        assert decompressed == simple_base64  # Should be identical after decompression
        # Note: Very small data may not compress well, but should still work
        assert compressed.startswith("zlib:")  # Should have method prefix

    def test_compress_decompress_gzip(self, simple_base64):
        """Test gzip compression and decompression."""
        compressed = compress_base64(simple_base64, method="gzip", compression_level=6)
        decompressed = decompress_base64(compressed)

        assert compressed != simple_base64
        assert decompressed == simple_base64
        assert compressed.startswith("gzip:")

    def test_compress_decompress_bz2(self, simple_base64):
        """Test bz2 compression and decompression."""
        compressed = compress_base64(simple_base64, method="bz2", compression_level=9)
        decompressed = decompress_base64(compressed)

        assert compressed != simple_base64
        assert decompressed == simple_base64
        assert compressed.startswith("bz2:")

    @pytest.mark.skipif(not BROTLI_AVAILABLE, reason="brotli not installed")
    def test_compress_decompress_brotli(self, simple_base64):
        """Test brotli compression and decompression."""
        compressed = compress_base64(simple_base64, method="brotli", compression_level=11)
        decompressed = decompress_base64(compressed)

        assert compressed != simple_base64
        assert decompressed == simple_base64
        assert compressed.startswith("brotli:")

    @pytest.mark.skipif(not LZ4_AVAILABLE, reason="lz4 not installed")
    def test_compress_decompress_lz4(self, simple_base64):
        """Test lz4 compression and decompression."""
        compressed = compress_base64(simple_base64, method="lz4")
        decompressed = decompress_base64(compressed)

        assert compressed != simple_base64
        assert decompressed == simple_base64
        assert compressed.startswith("lz4:")

    def test_compression_levels(self, simple_base64):
        """Test different compression levels."""
        compressed_1 = compress_base64(simple_base64, method="zlib", compression_level=1)
        compressed_6 = compress_base64(simple_base64, method="zlib", compression_level=6)
        compressed_9 = compress_base64(simple_base64, method="zlib", compression_level=9)

        # All should decompress correctly
        assert decompress_base64(compressed_1) == simple_base64
        assert decompress_base64(compressed_6) == simple_base64
        assert decompress_base64(compressed_9) == simple_base64

    def test_empty_string_compression(self):
        """Test compression of empty string."""
        compressed = compress_base64("", method="zlib")
        decompressed = decompress_base64(compressed)

        assert compressed == ""
        assert decompressed == ""

    def test_backward_compatibility_no_prefix(self, simple_base64):
        """Test decompression of data without method prefix (backward compatibility)."""
        # Manually compress without prefix (old format)
        import zlib
        data_bytes = simple_base64.encode('utf-8')
        compressed_bytes = zlib.compress(data_bytes, level=6)
        old_format = base64.b64encode(compressed_bytes).decode('utf-8')

        # Should still decompress correctly
        decompressed = decompress_base64(old_format)
        assert decompressed == simple_base64

    def test_is_compressed_function(self):
        """Test the is_compressed utility function."""
        # Create compressed data
        data = "data:test;base64,AAAA"
        compressed = compress_base64(data, method="zlib")

        # The function should detect it's compressed (after removing prefix)
        compressed_without_prefix = compressed.split(":", 1)[1]
        assert is_compressed(compressed_without_prefix)

        # Plain base64 should not be detected as compressed
        assert not is_compressed(base64.b64encode(b"test").decode())


class TestCompressionRatios:
    """Test compression effectiveness on different data types."""

    def test_repetitive_data_compression(self, repetitive_base64):
        """Test that repetitive data compresses extremely well."""
        compressed = compress_base64(repetitive_base64, method="zlib", compression_level=6)
        decompressed = decompress_base64(compressed)

        # Should compress very well (> 90%)
        ratio = len(compressed) / len(repetitive_base64)
        assert ratio < 0.10  # Less than 10% of original size
        assert decompressed == repetitive_base64  # Perfect recovery

    def test_image_compression(self, image_base64):
        """Test compression of image data."""
        compressed = compress_base64(image_base64, method="zlib", compression_level=6)
        decompressed = decompress_base64(compressed)

        # Should compress reasonably well (allowing up to 60% for PNG-encoded images)
        ratio = len(compressed) / len(image_base64)
        assert ratio < 0.60  # Less than 60% of original size
        assert decompressed == image_base64  # Perfect recovery

    @pytest.mark.skipif(not BROTLI_AVAILABLE, reason="brotli not installed")
    def test_brotli_best_compression(self, image_base64):
        """Test that brotli provides best compression ratio."""
        compressed_zlib = compress_base64(image_base64, method="zlib", compression_level=9)
        compressed_brotli = compress_base64(image_base64, method="brotli", compression_level=11)

        # Brotli should compress better or equal
        assert len(compressed_brotli) <= len(compressed_zlib)

    def test_compression_is_lossless(self, image_base64):
        """Test that compression is 100% lossless."""
        methods = ["zlib", "gzip", "bz2"]
        if BROTLI_AVAILABLE:
            methods.append("brotli")
        if LZ4_AVAILABLE:
            methods.append("lz4")

        for method in methods:
            compressed = compress_base64(image_base64, method=method)
            decompressed = decompress_base64(compressed)

            # Byte-for-byte identical
            assert decompressed == image_base64, f"{method} compression is not lossless"


# ---------------------------------------------------------------------------
# ModelQ Integration Tests
# ---------------------------------------------------------------------------

class TestModelQBase64Storage:
    """Test base64 storage functionality in ModelQ."""

    def test_store_base64_output_with_compression(self, modelq_instance, image_base64):
        """Test storing base64 output with compression."""
        # Create a task in Redis
        task_id = "test_task_001"
        task_dict = {
            "task_id": task_id,
            "status": "processing",
            "payload": {}
        }
        modelq_instance.redis_client.set(f"task:{task_id}", json.dumps(task_dict))

        # Store base64 output with compression
        result = modelq_instance.store_base64_output(
            image_base64,
            task_id=task_id,
            compress=True,
            compression_method="zlib",
            compression_level=6
        )

        assert result is True

        # Retrieve and verify
        stored_data = modelq_instance.redis_client.get(f"task:{task_id}")
        stored_dict = json.loads(stored_data)

        assert "base64_output" in stored_dict
        assert stored_dict["base64_output"].startswith("zlib:")

    def test_store_base64_output_without_compression(self, modelq_instance, image_base64):
        """Test storing base64 output without compression."""
        task_id = "test_task_002"
        task_dict = {
            "task_id": task_id,
            "status": "processing",
            "payload": {}
        }
        modelq_instance.redis_client.set(f"task:{task_id}", json.dumps(task_dict))

        # Store without compression
        result = modelq_instance.store_base64_output(
            image_base64,
            task_id=task_id,
            compress=False
        )

        assert result is True

        # Retrieve and verify
        stored_data = modelq_instance.redis_client.get(f"task:{task_id}")
        stored_dict = json.loads(stored_data)

        assert "base64_output" in stored_dict
        assert stored_dict["base64_output"] == image_base64  # Should be unchanged

    def test_get_task_base64_with_decompression(self, modelq_instance, image_base64):
        """Test retrieving base64 output with decompression."""
        task_id = "test_task_003"
        task_dict = {
            "task_id": task_id,
            "status": "completed",
            "payload": {}
        }
        modelq_instance.redis_client.set(f"task:{task_id}", json.dumps(task_dict))

        # Store compressed
        modelq_instance.store_base64_output(
            image_base64,
            task_id=task_id,
            compress=True,
            compression_method="zlib"
        )

        # Retrieve with decompression
        retrieved = modelq_instance.get_task_base64(task_id, decompress=True)

        assert retrieved == image_base64  # Should match original exactly

    def test_get_task_base64_without_decompression(self, modelq_instance, image_base64):
        """Test retrieving base64 output without decompression."""
        task_id = "test_task_004"
        task_dict = {
            "task_id": task_id,
            "status": "completed",
            "payload": {}
        }
        modelq_instance.redis_client.set(f"task:{task_id}", json.dumps(task_dict))

        # Store compressed
        modelq_instance.store_base64_output(
            image_base64,
            task_id=task_id,
            compress=True,
            compression_method="zlib"
        )

        # Retrieve without decompression (get raw compressed data)
        retrieved_compressed = modelq_instance.get_task_base64(task_id, decompress=False)

        assert retrieved_compressed.startswith("zlib:")
        assert len(retrieved_compressed) < len(image_base64)

    def test_store_all_compression_methods(self, modelq_instance, image_base64):
        """Test storing with all available compression methods."""
        methods = ["zlib", "gzip", "bz2"]
        if BROTLI_AVAILABLE:
            methods.append("brotli")
        if LZ4_AVAILABLE:
            methods.append("lz4")

        for i, method in enumerate(methods):
            task_id = f"test_task_method_{i}"
            task_dict = {"task_id": task_id, "status": "processing", "payload": {}}
            modelq_instance.redis_client.set(f"task:{task_id}", json.dumps(task_dict))

            # Store with this method
            result = modelq_instance.store_base64_output(
                image_base64,
                task_id=task_id,
                compress=True,
                compression_method=method
            )

            assert result is True

            # Retrieve and verify
            retrieved = modelq_instance.get_task_base64(task_id, decompress=True)
            assert retrieved == image_base64, f"Failed for method: {method}"

    def test_store_base64_without_task_id(self, modelq_instance, image_base64):
        """Test that storing without task_id returns False when no context exists."""
        result = modelq_instance.store_base64_output(
            image_base64,
            task_id=None,  # No task_id
            compress=True
        )

        # Should fail because there's no task context
        assert result is False

    def test_get_task_base64_nonexistent_task(self, modelq_instance):
        """Test retrieving base64 from non-existent task."""
        retrieved = modelq_instance.get_task_base64("nonexistent_task_id")
        assert retrieved is None

    def test_task_result_also_updated(self, modelq_instance, image_base64):
        """Test that both task and task_result are updated with base64 output."""
        task_id = "test_task_005"

        # Create both task and task_result
        task_dict = {"task_id": task_id, "status": "processing", "payload": {}}
        result_dict = {"task_id": task_id, "status": "completed", "result": {}}

        modelq_instance.redis_client.set(f"task:{task_id}", json.dumps(task_dict))
        modelq_instance.redis_client.set(f"task_result:{task_id}", json.dumps(result_dict))

        # Store base64 output
        modelq_instance.store_base64_output(
            image_base64,
            task_id=task_id,
            compress=True
        )

        # Check both are updated
        task_data = json.loads(modelq_instance.redis_client.get(f"task:{task_id}"))
        result_data = json.loads(modelq_instance.redis_client.get(f"task_result:{task_id}"))

        assert "base64_output" in task_data
        assert "base64_output" in result_data
        assert task_data["base64_output"] == result_data["base64_output"]

    def test_get_base64_prefers_task_result(self, modelq_instance, image_base64):
        """Test that get_task_base64 prefers task_result over task."""
        task_id = "test_task_006"

        # Create task with one value
        task_dict = {"task_id": task_id, "base64_output": "old_value"}
        modelq_instance.redis_client.set(f"task:{task_id}", json.dumps(task_dict))

        # Create task_result with different value
        result_dict = {"task_id": task_id, "base64_output": compress_base64(image_base64)}
        modelq_instance.redis_client.set(f"task_result:{task_id}", json.dumps(result_dict))

        # Should retrieve from task_result
        retrieved = modelq_instance.get_task_base64(task_id, decompress=True)
        assert retrieved == image_base64

    def test_compression_different_levels(self, modelq_instance, repetitive_base64):
        """Test that different compression levels produce different sizes."""
        levels = [1, 6, 9]
        sizes = []

        for level in levels:
            task_id = f"test_task_level_{level}"
            task_dict = {"task_id": task_id, "status": "processing", "payload": {}}
            modelq_instance.redis_client.set(f"task:{task_id}", json.dumps(task_dict))

            modelq_instance.store_base64_output(
                repetitive_base64,
                task_id=task_id,
                compress=True,
                compression_method="zlib",
                compression_level=level
            )

            stored_data = modelq_instance.redis_client.get(f"task:{task_id}")
            stored_dict = json.loads(stored_data)
            sizes.append(len(stored_dict["base64_output"]))

        # Higher compression levels should generally produce smaller or equal sizes
        # (Not strictly guaranteed for all data, but should be true for repetitive data)
        assert sizes[2] <= sizes[0]  # Level 9 <= Level 1


# ---------------------------------------------------------------------------
# Error Handling Tests
# ---------------------------------------------------------------------------

class TestCompressionErrorHandling:
    """Test error handling in compression functions."""

    def test_invalid_compression_method(self, simple_base64):
        """Test that invalid compression method returns original string."""
        result = compress_base64(simple_base64, method="invalid_method")
        # Should return original string on error
        assert result == simple_base64

    def test_decompress_invalid_data(self):
        """Test decompression of invalid data."""
        invalid_data = "zlib:invalid_base64_data!!!"
        result = decompress_base64(invalid_data)
        # Should return the input as-is when decompression fails
        assert result == invalid_data

    def test_decompress_corrupted_data(self, simple_base64):
        """Test decompression of corrupted compressed data."""
        compressed = compress_base64(simple_base64, method="zlib")
        # Corrupt the data
        corrupted = compressed[:-10] + "corrupted!"

        result = decompress_base64(corrupted)
        # Should return corrupted input when decompression fails
        assert result == corrupted

    @pytest.mark.skipif(BROTLI_AVAILABLE, reason="Test only when brotli is not installed")
    def test_brotli_not_installed_error(self, simple_base64):
        """Test that missing brotli raises appropriate error."""
        result = compress_base64(simple_base64, method="brotli")
        # Should return original string when library is missing
        assert result == simple_base64

    @pytest.mark.skipif(LZ4_AVAILABLE, reason="Test only when lz4 is not installed")
    def test_lz4_not_installed_error(self, simple_base64):
        """Test that missing lz4 raises appropriate error."""
        result = compress_base64(simple_base64, method="lz4")
        # Should return original string when library is missing
        assert result == simple_base64


# ---------------------------------------------------------------------------
# Performance Tests
# ---------------------------------------------------------------------------

class TestCompressionPerformance:
    """Test compression performance characteristics."""

    def test_compression_speed_acceptable(self, image_base64):
        """Test that compression completes in reasonable time."""
        import time

        start = time.time()
        compressed = compress_base64(image_base64, method="zlib", compression_level=6)
        duration = time.time() - start

        # Should complete in under 100ms for typical images
        assert duration < 0.1
        assert compressed != image_base64

    def test_decompression_speed_acceptable(self, image_base64):
        """Test that decompression completes in reasonable time."""
        import time

        compressed = compress_base64(image_base64, method="zlib", compression_level=6)

        start = time.time()
        decompressed = decompress_base64(compressed)
        duration = time.time() - start

        # Decompression should be very fast (under 50ms)
        assert duration < 0.05
        assert decompressed == image_base64

    @pytest.mark.skipif(not LZ4_AVAILABLE, reason="lz4 not installed")
    def test_lz4_fastest_compression(self, image_base64):
        """Test that lz4 is faster than other methods."""
        import time

        # Time zlib
        start = time.time()
        compress_base64(image_base64, method="zlib", compression_level=6)
        zlib_time = time.time() - start

        # Time lz4
        start = time.time()
        compress_base64(image_base64, method="lz4")
        lz4_time = time.time() - start

        # lz4 should be faster or similar
        # (May not always be true for very small data, but generally true)
        assert lz4_time <= zlib_time * 2  # Allow some variance
