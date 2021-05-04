import org.junit.platform.engine.support.hierarchical.OpenTest4JAwareThrowableCollector;

import java.util.ArrayList;
import java.util.Hashtable;

public class Table {
    private String tableName;
    private String clusteringColumn;
    // key:column Name value:type
    private ArrayList<Hashtable<String, String>> columnType = new ArrayList<>();
    private ArrayList<Hashtable<String, String>> columMax = new ArrayList<>();
    private ArrayList<Hashtable<String, String>> columMin = new ArrayList<>();
    private ArrayList<String> pages;

    public Table(String tableName) {
        this.tableName = tableName;
        pages = new ArrayList<String>();
    }

    public String getName() {
        return tableName;
    }

    public void addPage(String pageName) {

        pages.add(pageName);
    }

    public void removePage(String page) {
        pages.remove(page);
    }

    public ArrayList<String> getPages() {
        return pages;
    }


    public String getClusteringColumn() {
        return clusteringColumn;
    }

    public void setClusteringColumn(String clusteringColumn) {
        this.clusteringColumn = clusteringColumn;
    }

    public ArrayList<Hashtable<String, String>> getColumnsType() {
        return columnType;
    }


    public String getColumnType(String columnName) {
        String ret = null;
        for (Hashtable<String,String> col :
               columnType ) {
            String temp = col.get(columnName);
            if(temp!=null)
                ret=temp;

        }
        return ret;
    }

    public void setColumnType(ArrayList<Hashtable<String,String>> colsType){
        columnType.addAll(colsType);
    }
}


