/*
   Copyright (c) 2019 LinkedIn Corp.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// https://developers.google.com/protocol-buffers/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package com.linkedin.data.protobuf;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Function;


/**
 * Utility class for writing Protocol Buffers encoded binary data.
 */
public class ProtoWriter implements Closeable
{
  public static final int FIXED32_SIZE = 4;
  public static final int FIXED64_SIZE = 8;
  public static final int MAX_VARINT32_SIZE = 5;
  public static final int MAX_VARINT64_SIZE = 10;

  private final DataWriter _dataWriter;

  /**
   * Underlying writer to write data to different kinds of sinks.
   */
  public interface DataWriter extends Flushable, Closeable
  {
    /**
     * Write a single byte.
     */
    void writeByte(final byte value) throws IOException;

    /**
     * Write a fixed length 32-bit signed integer.
     */
    void writeFixedInt32(final int value) throws IOException;

    /**
     * Write a fixed length 64-bit signed integer.
     */
    void writeFixedInt64(final long value) throws IOException;

    /**
     * Write a variable length 32-bit signed integer.
     */
    void writeInt32(final int value) throws IOException;

    /**
     * Write a variable length 64-bit signed integer.
     */
    void writeInt64(final long value) throws IOException;

    /**
     * Write a variable length 32-bit unsigned integer.
     */
    void writeUInt32(int value) throws IOException;

    /**
     * Write a variable length 64-bit unsigned integer.
     */
    void writeUInt64(long value) throws IOException;

    /**
     * Write a String.
     */
    void writeString(String value, Function<Integer, Byte> leadingOrdinalGenerator,
        boolean tolerateInvalidSurrogatePairs) throws IOException;

    /**
     * Write a byte array slice.
     */
    void writeBytes(byte[] value, int offset, int length) throws IOException;
  }

  /**
   * Create a new {@code ProtoWriter} wrapping the given {@code OutputStream}.
   */
  public ProtoWriter(OutputStream out)
  {
    this(new OutputStreamWriter(out));
  }

  /**
   * Create a new {@code ProtoWriter} wrapping the given {@code OutputStream} with the given buffer size.
   */
  public ProtoWriter(OutputStream out, int bufferSize)
  {
    this(new OutputStreamWriter(out, bufferSize));
  }

  /**
   * Create a new {@code ProtoWriter} wrapping the given {@link DataWriter}.
   */
  public ProtoWriter(DataWriter dataWriter)
  {
    _dataWriter = dataWriter;
  }

  /**
   * Write a single byte.
   */
  public void writeByte(final byte value) throws IOException
  {
    _dataWriter.writeByte(value);
  }

  /**
   * Write a byte array.
   */
  public void writeBytes(final byte[] value) throws IOException
  {
    _dataWriter.writeBytes(value, 0, value.length);
  }

  /**
   * Write a byte array slice.
   */
  public void writeBytes(byte[] value, int offset, int length) throws IOException
  {
    _dataWriter.writeBytes(value, offset, length);
  }

  /**
   * Write a fixed length 32-bit signed integer.
   */
  public final void writeFixedInt32(final int value) throws IOException
  {
    _dataWriter.writeFixedInt32(value);
  }

  /**
   * Write a variable length 32-bit signed integer.
   */
  public final void writeInt32(final int value) throws IOException
  {
    _dataWriter.writeInt32(value);
  }

  /**
   * Write a fixed length 64-bit signed integer.
   */
  public final void writeFixedInt64(final long value) throws IOException
  {
    _dataWriter.writeFixedInt64(value);
  }

  /**
   * Write a variable length 64-bit signed integer.
   */
  public final void writeInt64(final long value) throws IOException
  {
    _dataWriter.writeInt64(value);
  }

  /**
   * Compute the number of bytes that would be needed to encode an unsigned 32-bit integer.
   */
  public static int computeUInt32Size(final int value)
  {
    if ((value & (~0 << 7)) == 0)
    {
      return 1;
    }

    if ((value & (~0 << 14)) == 0)
    {
      return 2;
    }

    if ((value & (~0 << 21)) == 0)
    {
      return 3;
    }

    if ((value & (~0 << 28)) == 0)
    {
      return 4;
    }

    return 5;
  }

  /**
   * Flush any buffered data to the underlying outputstream.
   */
  public void flush() throws IOException
  {
    _dataWriter.flush();
  }

  /**
   * Write a variable length 32-bit unsigned integer.
   */
  public void writeUInt32(int value) throws IOException
  {
    _dataWriter.writeUInt32(value);
  }

  /**
   * Write a variable length 64-bit unsigned integer.
   */
  public void writeUInt64(long value) throws IOException
  {
    _dataWriter.writeInt64(value);
  }

  /**
   * Write a String without any leading ordinal.
   */
  public void writeString(String value) throws IOException
  {
    _dataWriter.writeString(value, null, false);
  }

  /**
   * Write a String.
   */
  public void writeString(String value, Function<Integer, Byte> leadingOrdinalGenerator) throws IOException
  {
    _dataWriter.writeString(value, leadingOrdinalGenerator, false);
  }

  /**
   * Write a String.
   */
  public void writeString(String value, Function<Integer, Byte> leadingOrdinalGenerator,
      boolean tolerateInvalidSurrogatePairs) throws IOException
  {
    _dataWriter.writeString(value, leadingOrdinalGenerator, tolerateInvalidSurrogatePairs);
  }

  @Override
  public void close() throws IOException
  {
    _dataWriter.close();
  }
}
