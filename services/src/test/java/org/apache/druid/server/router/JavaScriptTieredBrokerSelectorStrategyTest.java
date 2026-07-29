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

package org.apache.druid.server.router;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.LinkedHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class JavaScriptTieredBrokerSelectorStrategyTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final TieredBrokerSelectorStrategy STRATEGY = new JavaScriptTieredBrokerSelectorStrategy(
      "function (config, query) { if (query.getAggregatorSpecs && query.getDimensionSpec && query.getDimensionSpec().getDimension() == 'bigdim' && query.getAggregatorSpecs().size() >= 3) { var size = config.getTierToBrokerMap().values().size(); if (size > 0) { return config.getTierToBrokerMap().values().toArray()[size-1] } else { return config.getDefaultBrokerServiceName() } } else { return null } }",
      JavaScriptConfig.getEnabledInstance()
  );

  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std().addValue(
            JavaScriptConfig.class,
            JavaScriptConfig.getEnabledInstance()
        )
    );

    Assert.assertEquals(
        STRATEGY,
        mapper.readValue(
            mapper.writeValueAsString(STRATEGY),
            JavaScriptTieredBrokerSelectorStrategy.class
        )
    );
  }

  @Test
  public void testDisabled() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std().addValue(
            JavaScriptConfig.class,
            new JavaScriptConfig(false)
        )
    );

    final String strategyString = mapper.writeValueAsString(STRATEGY);

    expectedException.expect(JsonMappingException.class);
    expectedException.expectCause(CoreMatchers.instanceOf(IllegalStateException.class));
    expectedException.expectMessage("JavaScript is disabled");

    mapper.readValue(strategyString, JavaScriptTieredBrokerSelectorStrategy.class);
  }

  @Test
  public void testGetBrokerServiceName()
  {
    final LinkedHashMap<String, String> tierBrokerMap = new LinkedHashMap<>();
    tierBrokerMap.put("fast", "druid/broker");
    tierBrokerMap.put("slow", "druid/slowBroker");

    final TieredBrokerConfig tieredBrokerConfig = new TieredBrokerConfig()
    {
      @Override
      public String getDefaultBrokerServiceName()
      {
        return "druid/broker";
      }

      @Override
      public LinkedHashMap<String, String> getTierToBrokerMap()
      {
        return tierBrokerMap;
      }
    };

    final TopNQueryBuilder queryBuilder = new TopNQueryBuilder().dataSource("test")
                                                                .intervals("2014/2015")
                                                                .dimension("bigdim")
                                                                .metric("count")
                                                                .threshold(1)
                                                                .aggregators(new CountAggregatorFactory("count"));

    Assert.assertEquals(
        Optional.absent(),
        STRATEGY.getBrokerServiceName(
            tieredBrokerConfig,
            queryBuilder.build()
        )
    );


    Assert.assertEquals(
        Optional.absent(),
        STRATEGY.getBrokerServiceName(
            tieredBrokerConfig,
            Druids.newTimeBoundaryQueryBuilder().dataSource("test").bound("maxTime").build()
        )
    );

    Assert.assertEquals(
        Optional.of("druid/slowBroker"),
        STRATEGY.getBrokerServiceName(
            tieredBrokerConfig,
            queryBuilder.aggregators(
                new CountAggregatorFactory("count"),
                new LongSumAggregatorFactory("longSum", "a"),
                new DoubleSumAggregatorFactory("doubleSum", "b")
            ).build()
        )
    );

    // in absence of tiers, expect the default
    tierBrokerMap.clear();
    Assert.assertEquals(
        Optional.of("druid/broker"),
        STRATEGY.getBrokerServiceName(
            tieredBrokerConfig,
            queryBuilder.aggregators(
                new CountAggregatorFactory("count"),
                new LongSumAggregatorFactory("longSum", "a"),
                new DoubleSumAggregatorFactory("doubleSum", "b")
            ).build()
        )
    );

  }

  @Test
  public void testGetBrokerServiceNameFromMultipleThreads() throws Exception
  {
    final TieredBrokerSelectorStrategy strategy = new JavaScriptTieredBrokerSelectorStrategy(
        "function (config, query) { return config.getDefaultBrokerServiceName() }",
        JavaScriptConfig.getEnabledInstance()
    );
    final BlockingTieredBrokerConfig tieredBrokerConfig = new BlockingTieredBrokerConfig();
    final Query<?> query = Druids.newTimeBoundaryQueryBuilder().dataSource("test").bound("maxTime").build();
    final ExecutorService firstExecutor = Execs.singleThreaded("js-selector-test-first-%d");
    final ExecutorService secondExecutor = Execs.singleThreaded("js-selector-test-second-%d");

    try {
      final Future<Optional<String>> firstResult = firstExecutor.submit(
          () -> strategy.getBrokerServiceName(tieredBrokerConfig, query)
      );
      Assert.assertTrue(tieredBrokerConfig.firstInvocation.await(5, TimeUnit.SECONDS));

      final Future<Optional<String>> secondResult = secondExecutor.submit(
          () -> strategy.getBrokerServiceName(tieredBrokerConfig, query)
      );
      Assert.assertTrue(tieredBrokerConfig.invocations.await(5, TimeUnit.SECONDS));
      tieredBrokerConfig.release.countDown();

      Assert.assertEquals(
          Optional.of("druid/broker"),
          firstResult.get()
      );
      Assert.assertEquals(
          Optional.of("druid/broker"),
          secondResult.get()
      );
    }
    finally {
      tieredBrokerConfig.release.countDown();
      firstExecutor.shutdownNow();
      secondExecutor.shutdownNow();
    }
  }

  public static class BlockingTieredBrokerConfig extends TieredBrokerConfig
  {
    private final CountDownLatch firstInvocation = new CountDownLatch(1);
    private final CountDownLatch invocations = new CountDownLatch(2);
    private final CountDownLatch release = new CountDownLatch(1);

    @Override
    public String getDefaultBrokerServiceName()
    {
      firstInvocation.countDown();
      invocations.countDown();
      try {
        release.await();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      return super.getDefaultBrokerServiceName();
    }
  }
}
