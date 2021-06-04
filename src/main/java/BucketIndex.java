import java.io.Serializable;
import java.util.Hashtable;

public class BucketIndex implements Serializable {
    String[] columns;
    String pageName;

    public BucketIndex(String[] columns, String pageName) {
        this.columns = columns;
        this.pageName = pageName;
    }

    public String getPageName() {
        return pageName;
    }

    public void setPageName(String pageName) {
        this.pageName = pageName;
    }


}
