public class IndexRange {
    String columnName;
    Comparable min;
    Comparable max;


    public IndexRange(String columnName, Comparable min, Comparable max) {
        this.columnName = columnName;
        this.min = min;
        this.max = max;
    }

    public Comparable getMin() {
        return min;
    }

    public void setMin(Comparable min) {
        this.min = min;
    }

    public Comparable getMax() {
        return max;
    }

    public void setMax(Comparable max) {
        this.max = max;
    }

    public boolean isInRange(Comparable key)
    {
        if(key.compareTo(max)<=0 && key.compareTo(min)>=0)
            return true;
        else
            return false;
    }

}
