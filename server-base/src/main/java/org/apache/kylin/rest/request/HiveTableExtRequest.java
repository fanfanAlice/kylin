package org.apache.kylin.rest.request;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HiveTableExtRequest extends HiveTableRequest {
    public HiveTableExtRequest() {
    }
    private String project;

    @JsonProperty("qualified_table_name")
    private String qualifiedTableName;

    private int rows;

    public HiveTableExtRequest(String project, String qualifiedTableName, int rows) {
        this.project = project;
        this.qualifiedTableName = qualifiedTableName;
        this.rows = rows;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getQualifiedTableName() {
        return qualifiedTableName;
    }

    public void setQualifiedTableName(String qualifiedTableName) {
        this.qualifiedTableName = qualifiedTableName;
    }

    public int getRows() {
        return rows;
    }

    public void setRows(int rows) {
        this.rows = rows;
    }
}
