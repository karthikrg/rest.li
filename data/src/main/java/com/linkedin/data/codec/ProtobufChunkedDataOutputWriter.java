package com.linkedin.data.codec;

import com.linkedin.data.collections.ChunkedDataOutput;
import com.linkedin.data.protobuf.ProtoWriter;
import com.linkedin.data.protobuf.Utf8Utils;
import java.io.EOFException;
import java.io.IOException;
import java.util.function.Function;


/**
 * A {@link ProtoWriter.DataWriter} implementation that can write to a {@link ChunkedDataOutput}
 */
class ProtobufChunkedDataOutputWriter implements ProtoWriter.DataWriter
{
  private final ChunkedDataOutput _output;
  private int _position;
  private byte[] _buffer;

  ProtobufChunkedDataOutputWriter(ChunkedDataOutput output)
  {
    _output = output;
    _position = 0;
    _buffer = output.createBuffer();
  }

  private void loadNextBuffer()
  {
    _output.queueBuffer(_buffer);
    _position = 0;
    _buffer = _output.createBuffer();
  }

  @Override
  public void writeByte(byte value) throws IOException
  {
    if (_position == _buffer.length)
    {
      loadNextBuffer();
    }

    _buffer[_position++] = value;
  }

  @Override
  public void writeFixedInt32(int value) throws IOException
  {
    if (_buffer.length - _position >= ProtoWriter.FIXED32_SIZE)
    {
      _buffer[_position++] = (byte) (value & 0xFF);
      _buffer[_position++] = (byte) ((value >> 8) & 0xFF);
      _buffer[_position++] = (byte) ((value >> 16) & 0xFF);
      _buffer[_position++] = (byte) ((value >> 24) & 0xFF);
    }
    else
    {
      writeByte((byte) (value & 0xFF));
      writeByte((byte) ((value >> 8) & 0xFF));
      writeByte((byte) ((value >> 16) & 0xFF));
      writeByte((byte) ((value >> 24) & 0xFF));
    }
  }

  @Override
  public void writeFixedInt64(long value) throws IOException
  {
    if (_buffer.length - _position >= ProtoWriter.FIXED64_SIZE)
    {
      _buffer[_position++] = (byte) ((int) value & 0xFF);
      _buffer[_position++] = (byte) ((int) (value >> 8) & 0xFF);
      _buffer[_position++] = (byte) ((int) (value >> 16) & 0xFF);
      _buffer[_position++] = (byte) ((int) (value >> 24) & 0xFF);
      _buffer[_position++] = (byte) ((int) (value >> 32) & 0xFF);
      _buffer[_position++] = (byte) ((int) (value >> 40) & 0xFF);
      _buffer[_position++] = (byte) ((int) (value >> 48) & 0xFF);
      _buffer[_position++] = (byte) ((int) (value >> 56) & 0xFF);
    }
    else
    {
      writeByte((byte) ((int) value & 0xFF));
      writeByte((byte) ((int) (value >> 8) & 0xFF));
      writeByte((byte) ((int) (value >> 16) & 0xFF));
      writeByte((byte) ((int) (value >> 24) & 0xFF));
      writeByte((byte) ((int) (value >> 32) & 0xFF));
      writeByte((byte) ((int) (value >> 40) & 0xFF));
      writeByte((byte) ((int) (value >> 48) & 0xFF));
      writeByte((byte) ((int) (value >> 56) & 0xFF));
    }
  }

  @Override
  public void writeInt32(int value) throws IOException
  {
    if (value >= 0)
    {
      writeUInt32(value);
    }
    else
    {
      // Must sign-extend.
      writeUInt64(value);
    }
  }

  @Override
  public void writeInt64(long value) throws IOException
  {
    writeUInt64(value);
  }

  @Override
  public void writeUInt32(int value) throws IOException
  {
    if (_buffer.length - _position >= ProtoWriter.MAX_VARINT32_SIZE)
    {
      while (true)
      {
        if ((value & ~0x7F) == 0)
        {
          _buffer[_position++] = (byte) value;
          return;
        }
        else
        {
          _buffer[_position++] = (byte) ((value & 0x7F) | 0x80);
          value >>>= 7;
        }
      }
    }
    else
    {
      while (true)
      {
        if ((value & ~0x7F) == 0)
        {
          writeByte((byte) value);
          return;
        }
        else
        {
          writeByte((byte) ((value & 0x7F) | 0x80));
          value >>>= 7;
        }
      }
    }
  }

  @Override
  public void writeUInt64(long value) throws IOException
  {
    if (_buffer.length - _position >= ProtoWriter.MAX_VARINT64_SIZE)
    {
      while (true)
      {
        if ((value & ~0x7FL) == 0)
        {
          _buffer[_position++] = (byte) value;
          return;
        }
        else
        {
          _buffer[_position++] = (byte) (((int) value & 0x7F) | 0x80);
          value >>>= 7;
        }
      }
    }
    else
    {
      while (true)
      {
        if ((value & ~0x7FL) == 0)
        {
          writeByte((byte) value);
          return;
        }
        else
        {
          writeByte((byte) (((int) value & 0x7F) | 0x80));
          value >>>= 7;
        }
      }
    }
  }

  @Override
  public void writeString(String value, Function<Integer, Byte> leadingOrdinalGenerator,
      boolean tolerateInvalidSurrogatePairs) throws IOException
  {
    // Based on whether a leading ordinal generator is provided or not, we need to budget 0 or 1 byte.
    final int leadingOrdinalLength = (leadingOrdinalGenerator == null) ? 0 : 1;

    // UTF-8 byte length of the string is at least its UTF-16 code unit length (value.length()),
    // and at most 3 times of it. We take advantage of this below.
    final int maxLength = value.length() * 3;
    final int maxLengthVarIntSize = ProtoWriter.computeUInt32Size(maxLength);

    final int maxStringLength = leadingOrdinalLength + maxLengthVarIntSize + maxLength;

    // If we are streaming and the potential length is too big to fit in our buffer, we take the
    // slower path.
    if (maxStringLength > _buffer.length - _position)
    {
      // Allocate a byte[] that we know can fit the string and encode into it. String.getBytes()
      // does the same internally and then does *another copy* to return a byte[] of exactly the
      // right size. We can skip that copy and just writeRawBytes up to the actualLength of the
      // UTF-8 encoded bytes.
      final byte[] encodedBytes = new byte[maxLength];
      int actualLength = Utf8Utils.encode(value, encodedBytes, 0, maxLength, tolerateInvalidSurrogatePairs);

      if (leadingOrdinalGenerator != null)
      {
        writeByte(leadingOrdinalGenerator.apply(actualLength));
      }

      writeUInt32(actualLength);
      writeBytes(encodedBytes, 0, actualLength);
      return;
    }

    final int oldPosition = _position;
    try
    {
      // Optimize for the case where we know this length results in a constant varint length as
      // this saves a pass for measuring the length of the string.
      final int minLengthVarIntSize = ProtoWriter.computeUInt32Size(value.length());

      if (minLengthVarIntSize == maxLengthVarIntSize)
      {
        _position = oldPosition + leadingOrdinalLength + minLengthVarIntSize;
        int newPosition = Utf8Utils.encode(value, _buffer, _position, _buffer.length - _position, tolerateInvalidSurrogatePairs);
        // Since this class is stateful and tracks the position, we rewind and store the state,
        // prepend the length, then reset it back to the end of the string.
        _position = oldPosition;
        int length = newPosition - oldPosition - leadingOrdinalLength - minLengthVarIntSize;

        if (leadingOrdinalGenerator != null)
        {
          _buffer[_position++] = leadingOrdinalGenerator.apply(length);
        }

        writeUInt32(length);
        _position = newPosition;
      }
      else
      {
        int length = Utf8Utils.encodedLength(value, tolerateInvalidSurrogatePairs);

        if (leadingOrdinalGenerator != null)
        {
          _buffer[_position++] = leadingOrdinalGenerator.apply(length);
        }

        writeUInt32(length);
        _position = Utf8Utils.encode(value, _buffer, _position, length, tolerateInvalidSurrogatePairs);
      }
    }
    catch (IllegalArgumentException e)
    {
      throw new IOException(e);
    }
    catch (IndexOutOfBoundsException e)
    {
      throw new EOFException(String.format("Pos: %d, limit: %d, len: %d", _position, _buffer.length, 1));
    }
  }

  @Override
  public void writeBytes(byte[] value, int offset, int length) throws IOException
  {
    if (_buffer.length - _position >= length)
    {
      // We have room in the current buffer.
      System.arraycopy(value, offset, _buffer, _position, length);
      _position += length;
    }
    else
    {
      // Write extends past current buffer. Keep writing chunk by chunk till exhausted.
      while (length > 0)
      {
        int lengthToWrite = Math.min(_buffer.length - _position, length);
        System.arraycopy(value, offset, _buffer, _position, lengthToWrite);
        _position += lengthToWrite;
        offset += lengthToWrite;
        length -= lengthToWrite;

        if (length > 0 && _position == _buffer.length)
        {
          loadNextBuffer();
        }
      }
    }
  }

  @Override
  public void close() throws IOException
  {
    _output.queueLastBuffer(_buffer, _position);
    _position = 0;
    _buffer = null;
  }

  @Override
  public void flush() throws IOException
  {
    // Nothing to do.
  }
}
