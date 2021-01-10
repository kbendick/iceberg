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

package org.apache.iceberg.transforms;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.False;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.TestHelpers.assertAndUnwrapUnbound;
import static org.apache.iceberg.expressions.Expressions.notStartsWith;
import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestNotStartsWith {

  private static final String COLUMN = "someStringCol";
  private static final Schema SCHEMA = new Schema(optional(1, COLUMN, Types.StringType.get()));

  @Test
  public void testTruncateProjections() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).truncate(COLUMN, 4).build();

    assertProjectionInclusive(spec, notStartsWith(COLUMN, "ab"), "ab", Expression.Operation.NOT_STARTS_WITH);
    assertProjectionInclusive(spec, notStartsWith(COLUMN, "abab"), "abab", Expression.Operation.NOT_STARTS_WITH);
    assertProjectionInclusive(spec, notStartsWith(COLUMN, "ababab"), "abab", Expression.Operation.NOT_STARTS_WITH);

    assertProjectionStrict(spec, notStartsWith(COLUMN, "ab"), "ab", Expression.Operation.NOT_STARTS_WITH);
    // TODO(kbendick) - Logically, is this rewrite valid for strict projections?
    assertProjectionStrict(spec, notStartsWith(COLUMN, "abab"), "abab", Expression.Operation.NOT_EQ);

    // Due to truncation, we can't know if any of the values start with this or not.
    Expression projection = Projections.strict(spec).project(notStartsWith(COLUMN, "ababab"));
    Assert.assertTrue(projection instanceof False);
  }

  @Test
  // TODO(kbendick) - Rename, consider helper for setting up prelude, and then
  //                  finalize on a message format (I somewhat prefer the one in here).
  public void testTruncateString() {
    Truncate<String> trunc = Truncate.get(Types.StringType.get(), 2);
    Expression expr = notStartsWith(COLUMN, "abcde");
    BoundPredicate<String> boundExpr = (BoundPredicate<String>) Binder.bind(SCHEMA.asStruct(),  expr, false);

    UnboundPredicate<String> projected = trunc.project(COLUMN, boundExpr);
    Evaluator evaluator = new Evaluator(SCHEMA.asStruct(), projected);

    Assert.assertFalse("notStartsWith(abcde, truncate(abcde,2))  => false",
            evaluator.eval(TestHelpers.Row.of("abcde")));

    Assert.assertFalse("notStartsWith(abcde, truncate(ab, 2)) => false",
            evaluator.eval(TestHelpers.Row.of("ab")));

    Assert.assertFalse("notStartsWith(abcde, truncate(abcdz, 2)) => false",
            evaluator.eval(TestHelpers.Row.of("abcdz")));

    // truncate(a, 2) notStartsWith abcde
    Assert.assertTrue("notStartsWith(abcde, truncate(a, 2)) => false",
            evaluator.eval(TestHelpers.Row.of("a")));
  }

  @Test
  public void testNotStartsWithWhenValueIsLongerThanWidth() {
    Truncate<String> trunc = Truncate.get(Types.StringType.get(), 1);
    Expression expr = notStartsWith(COLUMN, "ab");
    BoundPredicate<String> boundExpr = (BoundPredicate<String>) Binder.bind(SCHEMA.asStruct(),  expr, false);
    UnboundPredicate<String> projected = trunc.project(COLUMN, boundExpr);
    Evaluator evaluator = new Evaluator(SCHEMA.asStruct(), projected);

    Assert.assertFalse("notStartsWith(ab, truncate(abcde, 1)) => false",
            evaluator.eval(TestHelpers.Row.of("abcde")));

    // a notStartsWith a => false
    Assert.assertFalse("notStartsWith(ab, truncate(abc, 1)) => false",
            evaluator.eval(TestHelpers.Row.of("abc")));
  }

  @Test
  // TODO(kbendick) - Rename, consider helper for setting up prelude, and then
  //                  finalize on a message format (I somewhat prefer the one in here).
  public void testNotStartsWithWhenPredicateValueIsShorterThanTruncationWidth() {
    Truncate<String> trunc = Truncate.get(Types.StringType.get(), 16);
    Expression expr = notStartsWith(COLUMN, "ab");
    BoundPredicate<String> boundExpr = (BoundPredicate<String>) Binder.bind(SCHEMA.asStruct(),  expr, false);
    UnboundPredicate<String> projected = trunc.project(COLUMN, boundExpr);
    Evaluator evaluator = new Evaluator(SCHEMA.asStruct(), projected);

    Assert.assertFalse("truncate(abcde, 16) notStartsWith ab => true",
            evaluator.eval(TestHelpers.Row.of("abcde")));

    Assert.assertFalse("truncate(ab, 16) notStartsWith ab => false",
            evaluator.eval(TestHelpers.Row.of("ab")));

    Assert.assertTrue("truncate(a, 16) notStartsWith ab => true",
            evaluator.eval(TestHelpers.Row.of("a")));
  }

  @Test
  // TODO(kbendick) - Rename, consider helper for setting up prelude, and then
  //                  finalize on a message format (I somewhat prefer the one in here).
  public void testNotStartsWithWhenPredicateValueIsLongerThanTruncationWidth() {
    Truncate<String> trunc = Truncate.get(Types.StringType.get(), 2);
    Expression expr = notStartsWith(COLUMN, "abcdefg");
    BoundPredicate<String> boundExpr = (BoundPredicate<String>) Binder.bind(SCHEMA.asStruct(),  expr, false);
    UnboundPredicate<String> projected = trunc.project(COLUMN, boundExpr);
    Evaluator evaluator = new Evaluator(SCHEMA.asStruct(), projected);

    // ab notStartsWith abcdefg is true, but bc of the truncation this should return false
    // as obviously the value abcdefg could be inside of the partition due to the width.
    Assert.assertFalse("truncate(abcdefg, 2) notStartsWith abcdefg => true",
            evaluator.eval(TestHelpers.Row.of("abcdefg")));

    Assert.assertFalse("truncate(ab, 2) notStartsWith abcdefg => false",
            evaluator.eval(TestHelpers.Row.of("ab")));

    Assert.assertTrue("truncate(a, 16) notStartsWith abcdefg => true",
            evaluator.eval(TestHelpers.Row.of("a")));
  }

  // TODO(kbendick) - Maybe explicitly instantiate and test directly the various
  //                  forms of Evaluators (ResidualEvaluator, etc).

  private void assertProjectionInclusive(PartitionSpec spec, UnboundPredicate<?> filter,
                                       String expectedLiteral, Expression.Operation expectedOp) {
    Expression projection = Projections.inclusive(spec).project(filter);
    assertProjection(spec, expectedLiteral, projection, expectedOp);
  }

  private void assertProjectionStrict(PartitionSpec spec, UnboundPredicate<?> filter,
                                    String expectedLiteral, Expression.Operation expectedOp) {
    Expression projection = Projections.strict(spec).project(filter);
    assertProjection(spec, expectedLiteral, projection, expectedOp);
  }

  private void assertProjection(PartitionSpec spec, String expectedLiteral, Expression projection,
                              Expression.Operation expectedOp) {
    UnboundPredicate<?> predicate = assertAndUnwrapUnbound(projection);
    Literal literal = predicate.literal();
    Truncate<CharSequence> transform = (Truncate<CharSequence>) spec.getFieldsBySourceId(1).get(0).transform();
    String output = transform.toHumanString((String) literal.value());

    Assert.assertEquals(expectedOp, predicate.op());
    Assert.assertEquals(expectedLiteral, output);
  }
}
