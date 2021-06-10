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
    //[0-10,10-20,---][20-40,40-60,...][30-40,40-50,60-70]
    //[[0,1],[3,4],[3,4,5]
    //[0,3,3],[0
    //GI[1][2],GI[1][3],GI[1][4],GI
    //GI[1][0],GI[1][1],GI[2][0],GI
    //GI[1][0],GI[1][1],GI[
//    [ [0,1,2,3,4]          // 10-20 k
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


    // To do
    // read indices when opening database
    // fill index --test
    // fix overflow problems
    // insert to index
    Table table;
    Vector<IndexRange[]> rangesColumns = new Vector<>();
    String[] columnsIndexed ;
    Object[] gridIndex ;
    Vector<String> buckets ;
    int dimension;
    int bucketsNum;
    String fileName ="";

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

        fillIndex();
        DBApp.writeIndexToFile(this);


    }

    public void fillIndex() {
        ArrayList<String> allPagesNames = table.getPages();

        for (String pageName :
                allPagesNames) {
            addRecordsOfPageAndItsOverflowToIndex(pageName);

        }
    }

    private void addRecordsOfPageAndItsOverflowToIndex(String pageName) {
        String pagePath = DBApp.getPagePath(pageName);
        Vector<Hashtable<String,Object>> pageVector = DBApp.readVectorFromPageFile(pagePath);
        ArrayList<Vector<Hashtable<String, Object>>> overflows = DBApp.getOverflowPages(pageVector);

        addRecordsOfVectorToIndex(pageVector);

        for (Vector<Hashtable<String, Object>> overflow:
        overflows){
            addRecordsOfVectorToIndex(overflow);
        }

    }

    private void addRecordsOfVectorToIndex(Vector<Hashtable<String, Object>> pageVector) {

        Hashtable<String,Object> header = pageVector.get(0);
        String pageName = (String) header.get("pageName");

        for (int i = 1; i < pageVector.size(); i++) {
            Hashtable<String, Object> record = pageVector.get(i);
            this.inesrtRecordToIndex(record,pageName);
        }

    }

    private void setFileName() {
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
        intializeGridIndex(1,gridIndex);

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


    public Vector<Integer> getBucketIndices(Hashtable<String,Object> colNameValue)
    {
        Vector<Integer> indices = new Vector<>();
        Vector<Object> values = new Vector<>();
        for (String column :
                columnsIndexed) {
            values.add(colNameValue.get((column)));
        }

        for (int i = 0; i <rangesColumns.size(); i++) {
            Comparable value = (Comparable) values.get(i);
            IndexRange[] rangeArr = rangesColumns.get(i);
            getIndexRangefColumn(rangeArr,value,indices);

        }


        return indices;

    }


    public static void getIndexRangefColumn(IndexRange[] rangeArr,Comparable value,Vector<Integer> indices)
    {
        for (int j = 0; j <rangeArr.length; j++) {
            IndexRange range = rangeArr[j];
            if(range.isInRange(value))
            {
                indices.add(j);
                return;
            }

        }

    }


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
            int minInt = getSumUnicode((String)min);
            int maxInt = getSumUnicode((String)max);
            intializeColumnRangeNumerical(columnName,minInt,maxInt);
        }
        else
        {

        }

    }

    public static int getSumUnicode(String string) {
        int sumUnicode =0;

        for (int i = 0; i < string.length(); i++) {
            sumUnicode+=string.charAt(i);
        }
        return sumUnicode;
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
            //round to decimal
            minBoundary=Math.round(minBoundary*100.0)/100.0;
            arrayIndex++;
        }

        rangesColumns.add(columnRange);

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
            increment = (maxDouble-minDouble) / 10;

        }
        return increment;
    }



    //recursive method to get to the cell where the BucketIndex is
    // will return null if the cell is empty

    public String getBucketFromIndex(Vector<Integer> indices){

        Vector<Integer> indicesClone = new Vector<>();

        for (Integer i :
                indices) {
            indicesClone.add(i);
        }

        return getBucketFromIndexRec(indicesClone,gridIndex);
        
    }

    public String getBucketFromIndexRec(Vector<Integer> indices, Object[] array){

        if(indices.size()==1) {
            int index =indices.get(0);
            return (String) array[index];
        }
        int index = indices.remove(0);
        Object[] loopArray = (Object[]) array[index];

        return getBucketFromIndexRec(indices,loopArray);

    }


    public void deleteBucketFromIndex(Vector<Integer> indeces)
    {
        putBucketInCell(null,indeces);
    }

    public void putBucketInCell( String bucket,Vector<Integer> indeces)
    {
        putBucketInCellRec(indeces,bucket,gridIndex);
    }

    private void putBucketInCellRec(Vector<Integer> indeces, String bucketName, Object[] array) {
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

     void inesrtRecordToIndex(Hashtable<String,Object> record,String pageName)
    {
        Vector<Integer> indices =getBucketIndices(record);
        //bucketName may be null !!
        String bucketName = getBucketFromIndex(indices);
        Vector<Object> bucket = getBucketToInsertIn(bucketName,indices);

        BucketIndex bucketIndex = new BucketIndex(columnsIndexed,pageName);
        DBApp.insertToBucket(bucket,bucketIndex,this);



    }

    private Vector<Object> getBucketToInsertIn(String bucketName, Vector<Integer> indices) {
        Vector<Object> bucket = null;

        if (bucketName==null)
        {
            bucket=addBucketToIndex(indices);
        }
        else {
            bucket=readBucketFromFile(bucketName);
        }

        return bucket;
    }


    //creates buckets and inserts it to index
    Vector<Object> addBucketToIndex(Vector<Integer> indices)
    {
        Vector<Object> bucket = null;
        bucket= DBApp.createBucket(this);
        String bucketName = DBApp.getBucketNameFromHeader(bucket);
        putBucketInCell(bucketName,indices);

        return bucket;
    }


    Vector<Object> readBucketFromFile (String bucketName)
    {
        Vector bucket = null;
        String bucketPath = DBApp.getBucketPath(bucketName);
        bucket = DBApp.readVectorFromPageFile(bucketPath);

        return bucket;
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
