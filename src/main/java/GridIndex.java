import java.awt.image.AreaAveragingScaleFilter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;

public class GridIndex implements Serializable   {

    //[
    // Salary[ "10-20","20-30","30-40","50-60","60-70","70-80","100+" ],
    // ID[ "10-20","20-30","30-40","50-60","60-70","70-80","100+" ],


    // Select from t where Salary= 10 , Id = 50 , DOB  = 90 , Age 20
    // salary =>0 , ID=>3 ,DOB=>6

    //Grid index GI 4D array
    // GI[0][3][6][1]
 //   10-20, 20-30
//    [ [0,  1    ,2,3,4]          // 10-20 k
//      []          //20-30k
//      []
//      []
//      []
//      []      ]


//    bucket 0,0
    // ------------
    //| record1   | // student0 , 234
    //| record2   |
    //| record3   |
    //| record4   |
    // ------------
    Table table;
    Vector<IndexRange[]> rangesColumns = new Vector<>();
    String[] columnsIndexed ;
    Object[] gridIndex ;
    Vector<String> buckets ;
    int dimension;
    int bucketsNum;
    String fileName ;

    public GridIndex(Table table ,String[] columnsIndexed)
    {
        this.table=table;
        this.columnsIndexed=columnsIndexed;
        dimension= columnsIndexed.length;
        bucketsNum=0;
        buckets = new Vector<String>();
        setFileName();
        intializeRanges();
        intializeGridIndex();

        //fillIndex()

    }

    private void setFileName() {
        String fileName = "";
        String tableName = table.getName();
        fileName += tableName+"Index";

        for (String column :
                columnsIndexed) {
            fileName+="_"+column;
        }

        fileName+=".class";
        
    }

    public String getFileName()
    {
        return fileName;
    }

    private  void intializeGridIndex(){
        gridIndex= new Object[10];
        intializeGridIndex(0,gridIndex);

    }
    private void intializeGridIndex(int level, Object[] array) {

        if (level>=dimension)
            return;
        for (int i=0; i <10 ; i++) {
            Object[] tempArray = new Object[10];
            array[i]=tempArray;
            intializeGridIndex(level+1,tempArray);
        }



    }

    private void intializeRanges() {

        //get  min and max of each column
        for (String column :
                columnsIndexed) {
            Comparable min = table.getColumnMin(column);
            Comparable max = table.getColumnMax(column);
            String type = table.getColumnType(column);
            intializeColumnRange(column,type,min,max);

        }

    }


//    public ArrayList<Integer> getBucketIndeces(Hashtable<String,Object> colNameValue)
//    {
//
//    }



    //Should the min and max be based on the min and max in the CSV?
    //how to divide the range of a string ???
    public void intializeColumnRange(String columnName,String type,Comparable min,Comparable max){
        if(type.equals(DBApp.intType))
        {
            intializeColumnRangeNumerical(columnName,(int)min,(int)max);
        }
        else if (type.equals(DBApp.doubleType)){
            intializeColumnRangeNumerical(columnName,(double)min,(double)max);
        }
        else if (type.equals(DBApp.dateType))
        {

        }
        else if (type.equals(DBApp.stringType))
        {

        }
        else
        {

        }

    }

    private void intializeColumnRangeNumerical(String columnName, Number min, Number max) {


        IndexRange[] columnRange = new IndexRange[10];
        int arrayIndex = 0;
        double increment = getIncrementNumber(min,max);
        double minBoundary = min.doubleValue();
        double maxBoundary = max.doubleValue(); ;
        while(minBoundary<maxBoundary)
        {

            addRange(columnRange,arrayIndex,columnName,minBoundary,increment);
            minBoundary+=increment;
            arrayIndex++;
        }

    }

    private void addRange(IndexRange[] columnRange, int arrayIndex, String columnName, double bottomBoundary, double increment) {
        double topBoundary = bottomBoundary+increment;
        IndexRange range = new IndexRange(columnName,bottomBoundary,topBoundary);
        columnRange[arrayIndex]=range;
    }


    private double getIncrementNumber(Number min,Number max) {

        double increment;
        if (min instanceof Integer && max instanceof Integer) {
            int minInt = (int) min;
            int maxInt = (int) max;
            increment = ((maxInt - minInt) + 0.0) / 10;
        } else {
            double minDouble = (double) min;
            double maxDouble = (double) max;
            increment = (minDouble - maxDouble) / 10;

        }
        return increment;
    }


    //recursive method to get to the cell where the BucketIndex is
    // will return null if the cell is empty

    public String getBucketIndexFromIndex(ArrayList<Integer> indeces){

        return getBucketFromIndexRec(indeces,gridIndex);
        
    }

    public String getBucketFromIndexRec(ArrayList<Integer> indeces, Object[] array){

        if(indeces.size()==1) {
            int index =indeces.get(0);
            return (String) array[index];
        }
        int index = indeces.remove(0);
        Object[] loopArray = (Object[]) array[index];

        return getBucketFromIndexRec(indeces,loopArray);

    }


    public void deleteBucketFromIndex(ArrayList<Integer> indeces)
    {
        putBucketInCell(indeces,null);
    }

    public void putBucketInCell(ArrayList<Integer> indeces, String bucket)
    {
        putBucketInCellRec(indeces,bucket,gridIndex);
    }

    private void putBucketInCellRec(ArrayList<Integer> indeces, String bucketName, Object[] array) {
        if(indeces.size()==1)
        {
            int index=indeces.get(0);
            array[index]=bucketName;
            return;
        }
        else
        {
            int index = indeces.remove(0);
            Object[] loopArray = (Object[]) array[index];

            putBucketInCellRec(indeces,bucketName,loopArray);

        }
    }

    void addBucketToIndex(String bucket){
        buckets.add(bucket);
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Vector<IndexRange[]> getRangesColumns() {
        return rangesColumns;
    }

    public void setRangesColumns(Vector<IndexRange[]> rangesColumns) {
        this.rangesColumns = rangesColumns;
    }

    public String[] getColumnsIndexed() {
        return columnsIndexed;
    }

    public void setColumnsIndexed(String[] columnsIndexed) {
        this.columnsIndexed = columnsIndexed;
    }

    public Object[] getGridIndex() {
        return gridIndex;
    }

    public void setGridIndex(Object[] gridIndex) {
        this.gridIndex = gridIndex;
    }

    public int getDimension() {
        return dimension;
    }


    public int getBucketsNum() {
        return bucketsNum;
    }

    public void setBucketsNum(int bucketsNum) {
        this.bucketsNum = bucketsNum;
    }
}
