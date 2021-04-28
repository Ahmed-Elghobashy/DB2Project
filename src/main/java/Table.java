import org.junit.platform.engine.support.hierarchical.OpenTest4JAwareThrowableCollector;

import java.util.ArrayList;
import java.util.Hashtable;

public class Table {
    private String tableName;
    private String clusteringColumn;
    // key:column Name value:type
    private ArrayList<Hashtable<String,String>> columns = new ArrayList<>();
    private ArrayList<String> pages;

    public Table(String tableName){
        this.tableName=tableName;
        pages = new ArrayList<String>();
    }

    public String getName(){
        return tableName;
    }

    public void addPage(String pageName){

        pages.add(pageName);
    }

    public ArrayList<String> getPages(){
        return pages;
    }


    public String getClusteringColumn() {
        return clusteringColumn;
    }

    public void setClusteringColumn(String clusteringColumn) {
        this.clusteringColumn = clusteringColumn;
    }

    public ArrayList<Hashtable<String,String>> getColumns() {
        return columns;
    }
}
