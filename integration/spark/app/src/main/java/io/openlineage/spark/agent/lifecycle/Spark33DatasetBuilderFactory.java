/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.CommandPlanVisitor;
import io.openlineage.spark.agent.lifecycle.plan.SaveIntoDataSourceCommandVisitor;
import io.openlineage.spark.agent.util.DeltaUtils;
import io.openlineage.spark.agent.util.LakeSoulUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.*;
import io.openlineage.spark32.agent.lifecycle.plan.AlterTableCommandDatasetBuilder;
import io.openlineage.spark33.agent.lifecycle.plan.CreateReplaceDatasetBuilder;
import io.openlineage.spark33.agent.lifecycle.plan.ReplaceIcebergDataDatasetBuilder;
import java.util.Collection;
import java.util.List;

import io.openlineage.spark33.agent.lifecycle.plan.UpsertLakeSoulInputDatasetBuilder;
import io.openlineage.spark33.agent.lifecycle.plan.UpsertLakeSoulOutputDatasetBuilder;
import lombok.extern.slf4j.Slf4j;
import scala.PartialFunction;

@Slf4j
public class Spark33DatasetBuilderFactory extends Spark32DatasetBuilderFactory
    implements DatasetBuilderFactory {

  @Override
  public Collection<PartialFunction<Object, List<OpenLineage.InputDataset>>> getInputBuilders(
          OpenLineageContext context) {
    DatasetFactory<OpenLineage.InputDataset> datasetFactory = DatasetFactory.input(context);
    ImmutableList.Builder builder =
            ImmutableList.<PartialFunction<Object, List<OpenLineage.InputDataset>>>builder()
                    .add(new LogicalRelationDatasetBuilder(context, datasetFactory, true))
                    .add(new InMemoryRelationInputDatasetBuilder(context))
                    .add(new CommandPlanVisitor(context))
                    .add(new DataSourceV2ScanRelationInputDatasetBuilder(context, datasetFactory))
                    .add(new SubqueryAliasInputDatasetBuilder(context))
                    .add(new DataSourceV2RelationInputDatasetBuilder(context, datasetFactory));

    if (DeltaUtils.hasMergeIntoCommandClass()) {
      builder.add(new MergeIntoCommandInputDatasetBuilder(context));
    }

    if (LakeSoulUtils.hasUpsertCommandClass()) {
      builder.add(new UpsertLakeSoulInputDatasetBuilder(context));
    }

    return builder.build();
  }

  @Override
  public Collection<PartialFunction<Object, List<OpenLineage.OutputDataset>>> getOutputBuilders(
      OpenLineageContext context) {
    DatasetFactory<OpenLineage.OutputDataset> datasetFactory = DatasetFactory.output(context);
    ImmutableList.Builder builder =
        ImmutableList.<PartialFunction<Object, List<OpenLineage.OutputDataset>>>builder()
            .add(new LogicalRelationDatasetBuilder(context, datasetFactory, false))
            .add(new SaveIntoDataSourceCommandVisitor(context))
            .add(new AppendDataDatasetBuilder(context, datasetFactory))
            .add(new DataSourceV2RelationOutputDatasetBuilder(context, datasetFactory))
            .add(new TableContentChangeDatasetBuilder(context))
            .add(new SubqueryAliasOutputDatasetBuilder(context))
            .add(new CreateReplaceDatasetBuilder(context))
            .add(new AlterTableCommandDatasetBuilder(context));

    if (DeltaUtils.hasMergeIntoCommandClass()) {
      builder.add(new MergeIntoCommandOutputDatasetBuilder(context));
    }

    if (ReplaceIcebergDataDatasetBuilder.hasClasses()) {
      builder.add(new ReplaceIcebergDataDatasetBuilder(context));
    }

    if (LakeSoulUtils.hasUpsertCommandClass()) {
      builder.add(new UpsertLakeSoulOutputDatasetBuilder((context)));
    }

    return builder.build();
  }
}
