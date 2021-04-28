import java.io.*;
import java.util.*;

public class DBApp implements DBAppInterface{


    //naming conventions for page : [tablename][page_number].class
    // page header
    // Hashtable :: Key:overflowPageName , Value : "[].class"
    //              Key:maxKey        , Value:
    //              Key:minKey         ,Value:
    //              key:isOverflowOF   ,Value:

    //naming conventionforOverFlowPages  [tablename][page_number]_[overflow number].class

    private static  final String metadataCSVPath = "src/main/resources/metadata.csv" ;
    private static  final String pagesDirectoryPath = "src/main/resources/pages" ;
    private static int maxRows = 200;

    private static final ArrayList<Table> tables = new ArrayList<Table>();

    public void init() throws IOException {

        intializeTables();
        //setMaxMin

    }

   public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException, IOException, ClassNotFoundException {

        Table table= getTable(tableName);
        ArrayList<String>  pages = table.getPages();
        //check for errors in input



        //if  pages is empty (Create page and insert)
       if(pages.isEmpty()){
        insertIntoEmptyTable(table,colNameValue);
       }
        else {
            getPageToInsertIn(colNameValue,pages,table);
       }






   }

   public void createTable(String strTableName,
                            String strClusteringKeyColumn, Hashtable<String,String> htblColNameType,
                            Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax )
            throws DBAppException, IOException {
        //check if table name is unique
        writeCsvTable( strTableName,strClusteringKeyColumn, htblColNameType,htblColNameMin, htblColNameMax);

    }



    public void createIndex(String strTableName,String[] strarrColName)throws DBAppException{

       //update csvFile

    }


    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException
    {
        //update csvFile
    }


    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException{

        //delete csvFile
    }

    public Iterator  selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException{

       //check csv File to know if indexed or not
       return null;
    }




    void writeCsvTable(String strTableName,
                       String strClusteringKeyColumn, Hashtable<String,String> htblColNameType,
                       Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax ) throws IOException
    {

        //getting keys
        Enumeration<String> colNameEnumration = htblColNameType.keys();


        while(colNameEnumration.hasMoreElements()){


            String colName = colNameEnumration.nextElement();
            String colType = htblColNameType.get(colName);
            int colMin = Integer.parseInt( htblColNameMin.get(colName));
            int colMax =Integer.parseInt(htblColNameMax.get(colName));

            boolean isClusterKey  =colName.equals(strClusteringKeyColumn);
            boolean indexed = false;

            writeCsvColumn(strTableName,colName,colType,isClusterKey,indexed,colMin,colMax);


        }

    }

    void writeCsvColumn(String tableName,String columnName,String columnType,
                        boolean clusteringKey,boolean indexed,int min,int max )throws IOException
    {
        FileWriter csvWriter = new FileWriter(metadataCSVPath,true);

        csvWriter.append(tableName);
        csvWriter.append(",");
        csvWriter.append(columnName);
        csvWriter.append(",");
        csvWriter.append(columnType);
        csvWriter.append(",");
        if(clusteringKey) {
            csvWriter.append("TRUE");
            csvWriter.append(",");
        }
        else{
            csvWriter.append("FALSE");
            csvWriter.append(",");
        }

        if(indexed) {
            csvWriter.append("TRUE");
        }
        else{
            csvWriter.append("FALSE");
        }

        csvWriter.append("\n");


        csvWriter.flush();
        csvWriter.close();



    }

    public void intializeTables() throws IOException {
        //read csv file
        ArrayList<String[]> metadataCSV=readCSV(metadataCSVPath);

        //cache for tableNames
        ArrayList<String> tableCache = new ArrayList<String>();

        //for each Table in CSV file : construct new table and add its pages if it is not in the cache
        for (String[] tableCSV :metadataCSV) {
            Table newTable;
            if(tableCSV.length>0) {
                String tableName = tableCSV[0];
                if (!tableCache.contains(tableName)) {
                    newTable = new Table(tableName);
                    addPages(newTable);
                    setColumns(metadataCSV,newTable);
                    tableCache.add(newTable.getName());
                    tables.add(newTable);
                }
            }


        }
    }

    public static void setColumns(ArrayList<String[]> metadataCSV,Table table){
        for (String[] tableCSV :
                metadataCSV) {
            if(tableCSV.length>0){
                String tableName = table.getName();
                final int  TABLE_NAME= 0;
                final int  COLUMN_NAME= 1;
                final int COLUMN_TYPE=2;
                final int ClUSTERING_KEY=3;
                final int INDEXED=4;
                final int MIN=5;
                final int MAX =6;

                if(tableCSV[TABLE_NAME].equals(tableName))
                {
                    Hashtable<String,String> column = new Hashtable<>();
                    column.put(tableCSV[COLUMN_NAME],tableCSV[COLUMN_TYPE]);
                    ArrayList< Hashtable<String,String>> columns = table.getColumns();
                    if(tableCSV[ClUSTERING_KEY]=="TRUE"){
                        table.setClusteringColumn(tableCSV[ClUSTERING_KEY]);
                    }

                }




            }
        }

    }

    public static void addPages(Table table){
            String[] pages = listPages();

        for (int i = 0; i <pages.length ; i++) {
            if (checkPageTable(table,pages[i])) {
                table.addPage(pages[i]);
            }
        }

    }


    // if return -1 then all pages are full
    //otherwise returns the number of the first not full page
    public static boolean checkPagesIsNotFull(Table table) throws IOException, ClassNotFoundException {
        ArrayList<String> pages = table.getPages();

        for (int  i =0;i<pages.size();i++)
        {
          String page = pages.get(i);
          String pagePath = getPagePath(page);
          boolean pageIsFull =  readVectorFromPageFile(pagePath).size()>=maxRows ;
          if(!pageIsFull){
              return true;
          }

        }


        return false;
    }


    public static String getPageName(String tableName,int pageNumber){
        return tableName +""+pageNumber+".class";
    }

    public static void insertIntoEmptyTable(Table table, Hashtable<String,Object> colNameValue) throws IOException, ClassNotFoundException {
        String newPagePath = createPage(table);
        Vector<Hashtable<String,Object>> pageVector =  readVectorFromPageFile(newPagePath);
        pageVector.add(colNameValue);
        modifyHeader(pageVector,colNameValue,table);
        writeVectorToPageFile(pageVector,newPagePath);
    }

    public static void modifyHeader(Vector pageVector,Hashtable<String,Object> colNameValue,Table table){

        Hashtable<String,Object> header = (Hashtable<String, Object>) pageVector.get(0);
        String clusteringColumn  = table.getClusteringColumn();
        Comparable newTupleClusteringKey = (Comparable) colNameValue.get(clusteringColumn);
        Comparable maxKey = (Comparable) header.get("maxKey");
        Comparable minKey = (Comparable) header.get("minKey");

        if ( maxKey==null || minKey==null ){
            header.put("maxKey",newTupleClusteringKey);
            header.put("minKey",newTupleClusteringKey);
        }
        else
        {
            if(maxKey.compareTo(newTupleClusteringKey)<0){
                    header.put("maxKey",newTupleClusteringKey);
            }
            if(minKey.compareTo(newTupleClusteringKey)>0){
                header.put("minKey",newTupleClusteringKey);
            }
        }





    }


    //check if page is related to the table by the naming convention
    public static boolean checkPageTable(Table table,String pageName)
    {
        String  tableName = table.getName();
        return pageName.startsWith(tableName) ;
    }

    public static String[] listPages(){
        File pagesDir = new File(pagesDirectoryPath);
        String[] retArray = pagesDir.list();
        Arrays.sort(retArray);
        return retArray;
    }


    public ArrayList<String[]> readCSV(String path) throws IOException {
        BufferedReader csvReader = new BufferedReader(new FileReader(metadataCSVPath));
        ArrayList<String[]> csvFileData = new ArrayList<String[]>();
        String row ;
        while ((row = csvReader.readLine()) != null) {
            String[] data = row.split(",");

            csvFileData.add(data);
        }
        csvReader.close();
        return csvFileData;
        }



      public static Table getTable(String  tableName){

            for (int i = 0; i <tables.size() ; i++) {
                if(tables.get(i).getName().equals(tableName))
                {
                    return  tables.get(i);
                }

            }
            return null;
        }

        //return page path
        public static String  createPage(Table table) throws IOException {

            int pageNumber = table.getPages().size();
            String pagePath = getPagePath(table.getName(),pageNumber);
            createPageFile(pagePath);
            //create Vector and write  it to the page
            Vector<Hashtable<String,Object> > page = new Vector<Hashtable<String,Object>>();
            createHeaderForNewPage();
            table.addPage(getPageName(table.getName(),pageNumber));
            writeVectorToPageFile(page,getPagePath(table.getName(),pageNumber));

            return  pagePath;
        }

        public static void createHeaderForNewPage(){
        Hashtable<String,Object> header =  new Hashtable<String,Object>();
        header.put("overflowPageName",null);
        header.put("maxKey",null);
        header.put("minKey",null);
        header.put("isOverFlowOf",null);
        }

        //returns vector of page and  the next page
        public static ArrayList<Vector> getPageToInsertIn(Hashtable<String,Object>  colNameValue ,ArrayList<String>  pages ,Table table) throws IOException, ClassNotFoundException {

        ArrayList<Vector> retArray =  new ArrayList<>();
        for (int i = 0;i<pages.size();i++) {
                String page = pages.get(i);
                String pagePath=getPagePath(page);
                Vector<Hashtable<String,Object>> pageVector= readVectorFromPageFile(pagePath);
                if(checkPageInsertion(pageVector,colNameValue,table)){
                    retArray.add(pageVector);
                    i++;
                    if(i<pages.size())
                    {
                         page = pages.get(i);
                         pagePath=getPagePath(page);
                        Vector<Hashtable<String,Object>> nextPageVector = readVectorFromPageFile(pagePath);
                        retArray.add(nextPageVector);
                    }
                    break;
                }
            }

        return retArray;

        }

        public static boolean checkPageInsertion(Vector<Hashtable<String,Object>> pageVector,Hashtable colNameValue,Table table){
            String clusteringColumn =  table.getClusteringColumn();
            Hashtable<String,Object> header = pageVector.get(0);
            Comparable clusteringKey =(Comparable) colNameValue.get(clusteringColumn);
            Comparable minKey = (Comparable) header.get("minKey");
            Comparable maxKey = (Comparable) header.get("maxKey");
            boolean retBoolean = (minKey.compareTo(clusteringColumn)<=0 && maxKey.compareTo(clusteringColumn)>=0 );
            return retBoolean;

        }

    public static void createHeaderForOverflowPage(String parentPageName){
            Hashtable<String,Object> header =  new Hashtable<String,Object>();
            header.put("overflowPageName",null);
            header.put("maxKey",null);
            header.put("minKey",null);
            header.put("isOverFlowOf",parentPageName);


        }

        public static void createPageFile(String pagePath) throws IOException {


            File pageFile = new File(pagePath);
            pageFile.createNewFile();

        }



        public static void writeVectorToPageFile(Vector vector,String pagePath) throws IOException {
            FileOutputStream fileOut = new FileOutputStream(pagePath);
            ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
            objectOut.writeObject(vector);
            objectOut.close();


        }

        public static String getPagePath(String tableName,int pageNumber){

        return pagesDirectoryPath+"/"+tableName+""+pageNumber+".class";
        }

        public static  String getPagePath (String pageName){
        return  pagesDirectoryPath+"/"+pageName ;
        }


        public static Vector readVectorFromPageFile(String pagePath) throws IOException, ClassNotFoundException {
        FileInputStream fileIn =  new FileInputStream(pagePath);
        ObjectInputStream objectIn =  new ObjectInputStream(fileIn);
        Vector retVector = (Vector) objectIn.readObject();
         return retVector;
        }




    public static void main(String[] args) throws DBAppException, IOException, ClassNotFoundException {
        String strTableName = "Student";
        DBApp dbApp = new DBApp( );
//        Hashtable htblColNameType = new Hashtable( ); htblColNameType.put("id", "java.lang.Integer");
//        htblColNameType.put("name", "java.lang.String");
//        htblColNameType.put("gpa", "java.lang.double");
//        Hashtable<String,String> htblColNameMin = new Hashtable( );
//        Hashtable<String,String> htblColNameMax = new Hashtable( );
//        htblColNameMin.put("name","0");
//        htblColNameMin.put("gpa","0");
//        htblColNameMin.put("id","0");
//
//        htblColNameMax.put("name","0");
//        htblColNameMax.put("gpa","4");
//        htblColNameMax.put("id","313242");
//
//
//
//
//        dbApp.createTable( strTableName, "id", htblColNameType,htblColNameMin,htblColNameMax );
//          dbApp.init();
        Table test = new Table("Student");
        Hashtable<String,Object> testHash = new Hashtable<>();
        testHash.put("id",1);
        testHash.put("name","Ahmed");
        testHash.put("gpa","5.0");
        insertIntoEmptyTable(test,testHash);
        Vector<Hashtable<String,Object>> vector = readVectorFromPageFile(getPagePath("Student",0));
    }

}
