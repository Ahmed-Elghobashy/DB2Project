import java.util.Hashtable;

public class BucketIndex {
    String[] columns;
    String pageName;
    int index;

    public BucketIndex(String[] columns, String pageName, int index) {
        this.columns = columns;
        this.pageName = pageName;
        this.index = index;
    }

    public String getPageName() {
        return pageName;
    }

    public void setPageName(String pageName) {
        this.pageName = pageName;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
