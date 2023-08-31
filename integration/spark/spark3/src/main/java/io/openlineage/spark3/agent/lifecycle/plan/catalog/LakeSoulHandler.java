/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.api.OpenLineageContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog;

import java.util.Map;
import java.util.Optional;

public class LakeSoulHandler implements CatalogHandler {

    private final OpenLineageContext context;

    public LakeSoulHandler(OpenLineageContext context) {
        this.context = context;
    }

    @Override public boolean hasClasses() {
        try {
            LakeSoulHandler.class.getClassLoader().loadClass("org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog");
            return true;
        } catch (Exception e) {
            // swallow- we don't care
        }
        return false;
    }

    @Override public boolean isClass(TableCatalog tableCatalog) {
        return tableCatalog instanceof LakeSoulCatalog;
    }

    @Override
    public DatasetIdentifier getDatasetIdentifier(SparkSession session,
                                                  TableCatalog tableCatalog,
                                                  Identifier identifier,
                                                  Map<String, String> properties) {
        return new DatasetIdentifier(identifier.name(), identifier.namespace()[0]);
    }

    @Override
    public Optional<OpenLineage.StorageDatasetFacet> getStorageDatasetFacet(Map<String, String> properties) {
        return Optional.of(
                context.getOpenLineage().newStorageDatasetFacet("lakesoul", "parquet"));
    }

    @Override public String getName() {
        return "lakesoul";
    }
}
