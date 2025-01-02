package com.minhkakart.bigdata.cassandra;

import com.datastax.oss.driver.api.core.cql.Row;

import java.util.UUID;

@SuppressWarnings("unused")
public class TrainedTree {
    private final UUID id;
    private final int session;
    private final String tree_name;
    private final String value;

    public TrainedTree(Row row) {
        this.id = row.getUuid("id");
        this.session = row.getInt("session");
        this.tree_name = row.getString("tree_name");
        this.value = row.getString("value");
    }

    public UUID getId() {
        return id;
    }

    public int getSession() {
        return session;
    }

    public String getTree_name() {
        return tree_name;
    }

    public String getValue() {
        return value;
    }

    public String toString() {
        return "TrainedTree{" +
                "id=" + id +
                ", session=" + session +
                ", tree_name='" + tree_name + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
