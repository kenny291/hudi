/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.sources;

import java.util.ArrayList;
import java.util.stream.Collectors;
import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;

/**
 * DFS Source reads multiple HUDI table then merge into one table.
 * This source class assumed all child table has the same schema.
 * Use schema from latest avro file as schema reader if input schema is not provided.
 */

public class MultiHoodieIncrSource extends HoodieIncrSource {

  private static final Logger LOG = LogManager.getLogger(MultiHoodieIncrSource.class);

  static class Config {

    /** {@value #HOODIE_SRC_BASE_PATH} is the base-path for the source Hoodie table. */
    static final String HOODIE_SRC_BASE_PATH = "hoodie.deltastreamer.source.hoodieincr.path";

    /**
     * {@value #NUM_INSTANTS_PER_FETCH} allows the max number of instants whose changes can be
     * incrementally fetched.
     */
    static final String NUM_INSTANTS_PER_FETCH =
        "hoodie.deltastreamer.source.hoodieincr.num_instants";

    static final Integer DEFAULT_NUM_INSTANTS_PER_FETCH = 1;

    // allow reading from latest committed instant
    static final Boolean DEFAULT_READ_LATEST_INSTANT_ON_MISSING_CKPT = false;
  }

  public MultiHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
  }

  @Override
  public Pair<Option<Dataset<Row>>, String> fetchNextBatch(
      Option<String> lastCkptStr, long sourceLimit) {

    DataSourceUtils.checkRequiredProperties(
        props, Collections.singletonList(Config.HOODIE_SRC_BASE_PATH));

    int numInstantsPerFetch =
        props.getInteger(Config.NUM_INSTANTS_PER_FETCH, Config.DEFAULT_NUM_INSTANTS_PER_FETCH);
    boolean readLatestOnMissingCkpt = Config.DEFAULT_READ_LATEST_INSTANT_ON_MISSING_CKPT;

    String delimiterTable = ",";
    String[] srcPathList = props.getString(Config.HOODIE_SRC_BASE_PATH).split(delimiterTable);
    ArrayList<String> endptsList = new ArrayList<>();
    Dataset<Row> srcAll = sparkSession.emptyDataFrame();

    // set checkpoint for each source
    String[] lastCkptList = lastCkptStr.isPresent() ? lastCkptStr.get().split(delimiterTable) : new String[0];

    for (int srcIdx = 0; srcIdx < srcPathList.length; srcIdx++) {
      Option<String> lastCkptStrTb;
      if (lastCkptList.length == srcPathList.length) {
        lastCkptStrTb =
            lastCkptList != null ? Option.of(lastCkptList[srcIdx]) : Option.empty();
      } else {
        // in init mode, all table has the same checkpoint
        lastCkptStrTb = Option.of("0");
      }

      String srcPath = srcPathList[srcIdx];
      String[] srcTableElement = srcPath.split("/");
      String srcTable = srcTableElement[srcTableElement.length - 1]; // get table by leaf name

      // Use begin Instant if set and non-empty
      Option<String> beginInstant =
          lastCkptStrTb.isPresent()
              ? lastCkptStrTb.get().isEmpty() ? Option.empty() : lastCkptStrTb
              : Option.empty();

      Pair<String, String> instantEndpts =
          IncrSourceHelper.calculateBeginAndEndInstants(
              sparkContext, srcPath, numInstantsPerFetch, beginInstant, readLatestOnMissingCkpt);

      if (instantEndpts.getKey().equals(instantEndpts.getValue())) {
        LOG.warn("Table: " + srcIdx + " (" + srcTable + ")"
                + " already caught up. Begin Checkpoint was :" + instantEndpts.getKey());
        endptsList.add(instantEndpts.getKey());
        continue;

      } else {
        endptsList.add(instantEndpts.getRight());
      }

      // Do Incremental pull. Set end instant if available
      DataFrameReader reader =
          sparkSession
              .read()
              .format("org.apache.hudi")
              .option(
                  DataSourceReadOptions.QUERY_TYPE().key(),
                  DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
              .option(DataSourceReadOptions.BEGIN_INSTANTTIME().key(), instantEndpts.getLeft())
              .option(DataSourceReadOptions.END_INSTANTTIME().key(), instantEndpts.getRight());

      Dataset<Row> source = reader.load(srcPath);
      LOG.info("Table " + srcIdx + " (" + srcTable + ")" + " loaded from " + instantEndpts.getLeft()
              + " to " + instantEndpts.getRight());

      // Remove Hoodie meta columns except partition path from input source
      Dataset<Row> src =
          source.drop(
              HoodieRecord.HOODIE_META_COLUMNS.stream()
                  .filter(x -> !x.equals(HoodieRecord.PARTITION_PATH_METADATA_FIELD))
                  .toArray(String[]::new));

      // union small source tables
      srcAll = srcAll.isEmpty() ? src : srcAll.unionAll(src);
    }
    LOG.info("Marked checkpoint: " + endptsList);
    String instantEndptAll =
        endptsList.stream().map(Object::toString).collect(Collectors.joining(delimiterTable));

    if (srcAll.isEmpty()) {
      return Pair.of(Option.empty(), instantEndptAll);
    } else {
      return Pair.of(Option.of(srcAll), instantEndptAll);
    }
  }
}
