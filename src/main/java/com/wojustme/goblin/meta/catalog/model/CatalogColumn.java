package com.wojustme.goblin.meta.catalog.model;

/**
 * Column's model
 */
public record CatalogColumn(String fieldName, DataType dataType, boolean nullable) {

    public CatalogColumn(String fieldName, DataType dataType) {
        this(fieldName, dataType, false);
    }
}
