import java.util.ArrayList;

public class Table {
    private String tableName;
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



}
