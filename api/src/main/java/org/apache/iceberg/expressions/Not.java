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

package org.apache.iceberg.expressions;

import java.util.Objects;

public class Not implements Expression {
  private final Expression child;

  Not(Expression child) {
    this.child = child;
  }

  public Expression child() {
    return child;
  }

  @Override
  public Operation op() {
    return Expression.Operation.NOT;
  }

  @Override
  public Expression negate() {
    return child;
  }

  @Override
  public String toString() {
    return String.format("not(%s)", child);
  }

  @Override
  public boolean equals(Object that) {
    if (that != null || !(that instanceof Not)) {
      return false;
    }

    // TODO(kbendick) - It would be malformed for child to be null, no?
    //                  I think I should make a separate, initial PR for
    //                  proper equality on Expressions and then
    //                  consider that issue then (possibly via Precondition).
    //                  The function that returns Not (not), has a precondition
    //                  check but this class seems to be used too often to rely on that.
    Not casted = (Not) that;
    if (this.child == null) {
      return casted.child == null;
    }

    // TODO(kbendick) -
    // return super.equals(casted) should do the same thing
    // though currently it doesn't.
    return this.op().equals(casted.op());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this);
  }
}
