from databento.common.buffer import BinaryBuffer


class TestBinaryBuffer:
    def test_initialization(self):
        # Arrange, Act
        buffer = BinaryBuffer()

        # Assert
        assert buffer.raw == b""
        assert buffer.len == 0

    def test_clear_empty_buffer_does_nothing(self):
        # Arrange
        buffer = BinaryBuffer()

        # Act
        buffer.clear()

        # Assert
        assert buffer.raw == b""
        assert buffer.len == 0

    def test_write_to_buffer_appends_to_raw_bytes(self):
        # Arrange
        buffer = BinaryBuffer()
        data = b"hello, world!"

        # Act
        buffer.write(data)

        # Assert
        assert buffer.raw == data
        assert len(buffer) == len(data)

    def test_write_to_buffer_multiple_times_appends_to_raw_bytes(self):
        # Arrange
        buffer = BinaryBuffer()
        data = b"hello, world!"

        # Act
        buffer.write(data)
        buffer.write(data)
        buffer.write(data)

        # Assert
        assert buffer.raw == data * 3
        assert len(buffer) == len(data * 3)

    def test_clear_non_empty_buffer_clears_raw_bytes(self):
        # Arrange
        buffer = BinaryBuffer()
        data = b"hello, world!"

        buffer.write(data)
        buffer.write(data)
        buffer.write(data)

        # Act
        buffer.clear()

        # Assert
        assert buffer.raw == b""
        assert len(buffer) == 0
