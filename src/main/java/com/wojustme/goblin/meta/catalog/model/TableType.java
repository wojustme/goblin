package com.wojustme.goblin.meta.catalog.model;

public enum TableType {

    /**
     * It's normal entity table, which data should be stored.
     */
    ENTITY,

    /**
     * It's normal view, which has no any data. But it has view sql.
     */
    VIEW,

    /**
     * It's special view, which data should be stored. And it has view sql.
     */
    MATERIALIZED_VIEW,
    ;
}
