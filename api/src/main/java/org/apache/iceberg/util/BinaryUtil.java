/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.util;

import java.nio.ByteBuffer;
import java.util.Comparator;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class BinaryUtil {
  // not meant to be instantiated
  private BinaryUtil() {
  }

  /**
   * Evaluates a ByteBuffer to see if it begins with a given prefix.
   * <p>
   * This comparison is used when testing a value, typically an upperBound or lowerBound
   * from some metrics, against a given {@link org.apache.iceberg.expressions.Predicate}
   * when both the expression's {@link org.apache.iceberg.expressions.Term}, represented by
   * {@param prefix}, and the value being tested, {@param value}, are best compared using
   * their ByteBuffer representation. This is the case for values of type
   * {@link org.apache.iceberg.types.Types.FixedType}, {@link org.apache.iceberg.types.Types.StringType},
   * {@link org.apache.iceberg.types.Types.BinaryType}.
   * <p>
   * This method assumes that both {@param value} and the ByteBuffer from {@param prefix} have their
   * current position at 0, or more exactly, have their position at the location for which we'd
   * like to search for the remaining bytes of {@param prefix} in {@param value}.
   * This assumption is typically valid when evaluating values of type
   * {@link org.apache.iceberg.expressions.BoundLiteralPredicate}
   * <p>
   * The prefix is used to obtain the correct {@link Comparator}, though we assume
   *
   * @param value ByteBuffer to check for {@param prefix}
   * @param prefix Literal the predicate term to search for when testing {@param value}
   * @return true if {@param value} startsWith {@param prefix}.
   */
  public static boolean startsWith(ByteBuffer value, Literal<ByteBuffer> prefix) {
    Preconditions.checkNotNull(value, "Cannot compare a null ByteBuffer");
    Preconditions.checkNotNull(prefix, "Cannot search for a null prefix in a ByteBuffer");
    ByteBuffer prefixAsBytes = prefix.toByteBuffer();
    Comparator<ByteBuffer> cmp = prefix.comparator();
    int length = Math.min(prefixAsBytes.remaining(), value.remaining());
    // truncate value so that its length in bytes is not greater than the length of prefix
    return cmp.compare(truncateBinary(value, length), prefixAsBytes) == 0;
  }


  /**
   * Truncates the input byte buffer to the given length
   */
  public static ByteBuffer truncateBinary(ByteBuffer input, int length) {
    Preconditions.checkArgument(length > 0, "Truncate length should be positive");
    if (length >= input.remaining()) {
      return input;
    }
    byte[] array = new byte[length];
    input.duplicate().get(array);
    return ByteBuffer.wrap(array);
  }

  /**
   * Returns a byte buffer whose length is lesser than or equal to truncateLength and is lower than the given input
   */
  public static Literal<ByteBuffer> truncateBinaryMin(Literal<ByteBuffer> input, int length) {
    ByteBuffer inputBuffer = input.value();
    if (length >= inputBuffer.remaining()) {
      return input;
    }
    return Literal.of(truncateBinary(inputBuffer, length));
  }

  /**
   * Returns a byte buffer whose length is lesser than or equal to truncateLength and is greater than the given input
   */
  public static Literal<ByteBuffer> truncateBinaryMax(Literal<ByteBuffer> input, int length) {
    ByteBuffer inputBuffer = input.value();
    if (length >= inputBuffer.remaining()) {
      return input;
    }

    // Truncate the input to the specified truncate length.
    ByteBuffer truncatedInput = truncateBinary(inputBuffer, length);

    // Try incrementing the bytes from the end. If all bytes overflow after incrementing, then return null
    for (int i = length - 1; i >= 0; --i) {
      byte element = truncatedInput.get(i);
      element = (byte) (element + 1);
      if (element != 0) { // No overflow
        truncatedInput.put(i, element);
        // Return a byte buffer whose position is zero and limit is i + 1
        truncatedInput.position(0);
        truncatedInput.limit(i + 1);
        return Literal.of(truncatedInput);
      }
    }
    return null; // Cannot find a valid upper bound
  }
}
