package simpledb.common;


import simpledb.storage.DbFile;

public class Table {
    /*
     * This data structure origins from the annotation in Catalog.java(method: addTable)
     */
    private DbFile dbFile;
    private String tableName;
    private String pkeyField;

    public Table(DbFile dbFile, String tableName, String pkeyField) {
        this.dbFile = dbFile;
        this.tableName = tableName;
        this.pkeyField = pkeyField;
    }

    public DbFile getDbFile() {
        return dbFile;
    }

    public void setDbFile(DbFile dbFile) {
        this.dbFile = dbFile;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPkeyField() {
        return pkeyField;
    }

    public void setPkeyField(String pkeyField) {
        this.pkeyField = pkeyField;
    }
}
