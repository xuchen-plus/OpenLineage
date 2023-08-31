/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark33.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias;
import org.apache.spark.sql.lakesoul.commands.UpsertCommand;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class UpsertLakeSoulOutputDatasetBuilder extends AbstractQueryPlanOutputDatasetBuilder<UpsertCommand> {
    public UpsertLakeSoulOutputDatasetBuilder(OpenLineageContext context) {
        super(context, false);
    }

    @Override protected boolean isDefinedAtLogicalPlan(LogicalPlan x) {
        return x instanceof UpsertCommand;
    }

    @Override protected List<OpenLineage.OutputDataset> apply(SparkListenerEvent event, UpsertCommand x) {
        LogicalPlan target = x.target();
        if (target instanceof SubqueryAlias) {
            target = ((SubqueryAlias) target).child();
        }

        return delegate(
                context.getOutputDatasetQueryPlanVisitors(), context.getOutputDatasetBuilders(), event)
                .applyOrElse(
                        target, ScalaConversionUtils.toScalaFn((lp) -> Collections.<OpenLineage.OutputDataset>emptyList()))
                .stream()
                .collect(Collectors.toList());
    }
}
