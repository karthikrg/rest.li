package com.linkedin.data.collections;

import com.linkedin.data.ByteString;
import java.util.Iterator;
import java.util.LinkedList;


/**
 * Represents an output sink that is backed by several homogeneous chunks of {@link byte[]}.
 */
public class ChunkedDataOutput
{
  private static final int DEFAULT_BUFFER_SIZE = 4096;

  private final LinkedList<byte[]> _buffers;
  private final int _bufferSize;

  private int _lastChunkPosition = 0;
  private int _size = 0;

  public ChunkedDataOutput()
  {
    this(DEFAULT_BUFFER_SIZE);
  }

  public ChunkedDataOutput(int bufferSize)
  {
    _bufferSize = bufferSize;
    _buffers = new LinkedList<>();
  }

  public byte[] createBuffer()
  {
    return new byte[_bufferSize];
  }

  public void queueBuffer(byte[] buffer)
  {
    assert buffer.length == _bufferSize;
    _buffers.add(buffer);
    _size += _bufferSize;
  }

  public void queueLastBuffer(byte[] buffer, int writtenLength)
  {
    assert buffer.length == _bufferSize;
    assert writtenLength <= _bufferSize;
    _buffers.add(buffer);
    _lastChunkPosition = writtenLength;
    _size += writtenLength;
  }

  public ByteString toUnsafeByteString()
  {
    if (_buffers.peekFirst() == null)
    {
      // Return an empty ByteString if the output stream is empty
      return ByteString.empty();
    }

    return new ByteString(_buffers, _lastChunkPosition);
  }

  public byte[] toByteArray()
  {
    if (_buffers.peekFirst() == null)
    {
      // Return an empty array if the output stream is empty
      return new byte[0];
    }

    byte[] targetBuffer = new byte[_size];
    int pos = 0;
    Iterator<byte[]> iter = _buffers.iterator();
    while (iter.hasNext())
    {
      byte[] buffer = iter.next();
      if (iter.hasNext())
      {
        // If it has next buffer, we know this buffer is full and
        // we copy the whole buffer to the new buffer.
        System.arraycopy(buffer, 0, targetBuffer, pos, buffer.length);
        pos += buffer.length;
      }
      else
      {
        // If this is the last buffer, we only copy valid content based on _lastChunkPosition.
        System.arraycopy(buffer, 0, targetBuffer, pos, _lastChunkPosition);
      }
    }
    return targetBuffer;
  }
}
