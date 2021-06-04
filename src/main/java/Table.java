import org.junit.platform.engine.support.hierarchical.OpenTest4JAwareThrowableCollector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;

public class Table implements Serializable {
    private String tableName;
    private String clusteringColumn;
    // max min
    // key:column Name value:type
    private ArrayList<Hashtable<String, String>> columnType = new ArrayList<>();
    private ArrayList<Hashtable<String, Boolean>> columnIndexed = new ArrayList<>();
    private ArrayList<Hashtable<String, Comparable>> columMax = new ArrayList<>();
    private ArrayList<Hashtable<String, Comparable>> columMin = new ArrayList<>();
    private ArrayList<String> pages;

    public Table(String tableName) {
        this.tableName = tableName;
        pages = new ArrayList<String>();
    }

    public String getName() {
        return tableName;
    }

    public ArrayList<Hashtable<String, Comparable>> getColumnsMax() {
        return columMax;
    }

    public ArrayList<Hashtable<String, Comparable>> getColumnsMin() {
        return columMin;
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


    public Comparable getColumnMax(String columnName){
        Comparable ret = null;
        for (Hashtable<String,Comparable> col :
                columMax ) {
            Comparable temp = col.get(columnName);
            if(temp!=null)
                ret=temp;

        }
        return ret;


    }

    public Comparable getColumnMin(String columnName){
        Comparable ret = null;
        for (Hashtable<String,Comparable> col :
                columMin ) {
            Comparable temp = col.get(columnName);
            if(temp!=null)
                ret=temp;

        }
        return ret;

    }


    public ArrayList<Hashtable<String, Boolean>> getColumnIndexed() {
        return columnIndexed;
    }

    public void setColumnIndexed(ArrayList<Hashtable<String, Boolean>> columnIndexed) {
        this.columnIndexed = columnIndexed;
    }

    public void setColumnType(ArrayList<Hashtable<String,String>> colsType){
        columnType.addAll(colsType);
    }
}


