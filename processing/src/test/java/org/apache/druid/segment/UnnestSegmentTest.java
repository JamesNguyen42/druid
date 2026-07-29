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

package org.apache.druid.segment;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

public class UnnestSegmentTest
{
  @Test
  public void testTimeBoundaryInspectorWrapsBaseSegmentInspector()
  {
    final DateTime minTime = DateTimes.of("2024-01-01");
    final DateTime maxTime = DateTimes.of("2024-01-02");
    final TimeBoundaryInspector baseInspector = new TimeBoundaryInspector()
    {
      @Override
      public DateTime getMinTime()
      {
        return minTime;
      }

      @Override
      public DateTime getMaxTime()
      {
        return maxTime;
      }

      @Override
      public boolean isMinMaxExact()
      {
        return true;
      }
    };
    final Segment baseSegment = new TestSegmentForAs(
        SegmentId.dummy("test"),
        clazz -> TimeBoundaryInspector.class.equals(clazz) ? baseInspector : null
    );

    final UnnestSegment segment = new UnnestSegment(
        baseSegment,
        new ExpressionVirtualColumn(
            "unnested",
            "\"values\"",
            ColumnType.STRING,
            TestExprMacroTable.INSTANCE
        ),
        null
    );

    final TimeBoundaryInspector inspector = segment.as(TimeBoundaryInspector.class);
    Assert.assertNotNull(inspector);
    Assert.assertEquals(minTime, inspector.getMinTime());
    Assert.assertEquals(maxTime, inspector.getMaxTime());
    Assert.assertFalse(inspector.isMinMaxExact());
  }
}
