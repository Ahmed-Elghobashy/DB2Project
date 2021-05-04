import java.io.*;
import java.nio.file.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DBApp implements DBAppInterface{

    //page name
    //update
    //naming conventions for page : [tablename][page_number].class
    // page header
    // Hashtable :: Key:overflowPageName , Value : "[].class"
    //              Key:maxKey        , Value:
    //              Key:minKey         ,Value:
    //              key:OverflowOf   ,Value: ".class"
    //              key:pageName ,     Value: "[].class"
    // if page is main page (not overflow) the maxKey and minKey is of the page and all its overflow , while

    //notes : we read pages to get the min and max

   // to do
   // read config--
   // Check inputs and throw input and throw exceptions
   // test getPagesToInsertIn method
   // test testBinarySearch--
   // test getOverFlowPages
   // test insert To non-full page(main page)
   // test insert To full page with the next page not full
   // test insertAndShift
   // test createOverflow and insert
   // modify parent header after insertion to overflow  page
   // test update


    //low+1 == high
   // 0 10 20 30 40 50 60 70 80 90 100


    //does the header count in the max rows ?



    //naming conventionforOverFlowPages  [tablename][page_number]_overflow_[overflow number].class

    private static  final String metadataCSVPath = "src/main/resources/metadata.csv" ;
    private static  final String pagesDirectoryPath = "src/main/resources/data" ;
    private static int maxRows ;
    private static int maxIndexBucket;

    private static final ArrayList<Table> tables = new ArrayList<Table>();

    public void init()  {


        try {
            intializeTables();
        } catch (IOException e) {
            e.printStackTrace();
        }
        setConfig();

        try {
            intializeTables();
        } catch (IOException e) {
            e.printStackTrace();
        }
        setConfig();
    }



   public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException, IOException, ClassNotFoundException {
       if(checkInputs(tableName,colNameValue)==false){
           throw new DBAppException();
       }
        if(checkInputs(tableName,colNameValue)==false){
            throw new DBAppException();
        }
        Table table= getTable(tableName);

        //when trying to insert into a table that doesn't exist
        if (table  == null)
            throw new DBAppException();

        ArrayList<String>  pages = table.getPages();
        String clusteringColumn = table.getClusteringColumn();
        Comparable insertKey =(Comparable) colNameValue.get(clusteringColumn);
        //check for errors in input



        //if no pages exist(The table is empty), Create page and insert
       if(pages.isEmpty()){
        insertIntoEmptyTable(table,colNameValue);
       }
        else {
           ArrayList<Vector> pagesToInsertIn = getPageToInsertIn(colNameValue,pages,table);
           
           if(pagesToInsertIn.size()==0)
               throw new DBAppException();

           Vector<Hashtable<String,Object>> mainPageVector = pagesToInsertIn.get(0);
           ArrayList<Vector<Hashtable<String,Object>>> overFlowPagesOfMainPage = getOverflowPages(mainPageVector);

           //when trying to insert a key that already exists
           if (pagesContainKey(overFlowPagesOfMainPage,insertKey,clusteringColumn))
               throw new DBAppException();

           // if the page we are trying to insert in is not full
           if(!checkIfPageIsFull(mainPageVector)){

               insertToPage(mainPageVector,colNameValue,table);


           }
           else {
               //else if  the page is  full we check if it has overflowPages
               if(overFlowPagesOfMainPage.size()>1)
               {
                 Vector<Hashtable<String,Object>> overflowPageVector = overFlowPagesOfMainPage.get(0);
                 if(!checkIfPageIsFull(overflowPageVector)){
                     insertToPage(overflowPageVector,colNameValue,table);
                     modifyMainPageVectorHeadeAfterInsertOverflow(mainPageVector,colNameValue,table);
                     writeVectorToPageFile(mainPageVector);
                 }
                 else{
                     Hashtable<String,Object> tempOverflowRecord = colNameValue;

                     for (int i = 0; i <overFlowPagesOfMainPage.size() ; i++) {
                         Vector<Hashtable<String,Object>> currentPageVector = overFlowPagesOfMainPage.get(i);
                        if(i==overFlowPagesOfMainPage.size()-1){
                            //create new overflow page and shift to it
                            String newOverflowName = createOverflowPage(table,currentPageVector);
                            String overflowPath = getPagePath(newOverflowName);
                            Vector<Hashtable<String,Object>> newOverflowVector = readVectorFromPageFile(overflowPath);
                            insertToPageAndShift(currentPageVector,newOverflowVector,colNameValue,table);
                            writeVectorToPageFile(currentPageVector);
                            writeVectorToPageFile(newOverflowVector);
                            break;
                        }
                         Vector<Hashtable<String,Object>> nextPageVector = overFlowPagesOfMainPage.get(i+1);
                         tempOverflowRecord = insertToPageAndShift(currentPageVector,nextPageVector,tempOverflowRecord,table);
                         writeVectorToPageFile(currentPageVector);

                     }
                     modifyMainPageVectorHeadeAfterInsertOverflow(mainPageVector,colNameValue,table);
                     writeVectorToPageFile(mainPageVector);

                 }
               }
               //if the page is full and doesn't have overflow pages we check if there is a page after it
               else if(pagesToInsertIn.size()>1)
               {
                   Vector<Hashtable<String,Object>> nextPageVector = pagesToInsertIn.get(1);
                   //check if the next page is not full
                   // if full => make overflow page and insert into it
                   if(checkIfPageIsFull(nextPageVector)){
                        //create overFlowPage
                       String overFlowPagePath = createOverflowPage(table,mainPageVector);
                        //read overflowPage
                       Vector<Hashtable<String,Object>> overflowPageVector = readVectorFromPageFile(overFlowPagePath);
                        //insert to overflow page
                       insertToVector(overflowPageVector,colNameValue,clusteringColumn);
                       modifyHeaderInsert(overflowPageVector,table);
                       modifyMainPageVectorHeadeAfterInsertOverflow(mainPageVector,colNameValue,table);
                       writeVectorToPageFile(mainPageVector);
                       writeVectorToPageFile(overflowPageVector);

                       //modifyHeaderOfParent
                   }
                   // if not full => insert to main page and shift overflow record to next page
                   else {
//                       insertToVector(mainPageVector,colNameValue,clusteringColumn);
//                       Hashtable<String,Object> overflowRecord=mainPageVector.lastElement();
//                       mainPageVector.remove(overflowRecord);
//                       insertToVector(nextPageVector,overflowRecord,clusteringColumn);
//
//                       modifyHeaderInsert(mainPageVector,table);
//                       modifyHeaderInsert(nextPageVector,table);
                       insertToPageAndShift(mainPageVector,nextPageVector,colNameValue,table);
                       writeVectorToPageFile(nextPageVector);
                       writeVectorToPageFile(mainPageVector);

                   }
               }
               //if it is full and has no page after it create a page and insert in it
               else {
                    String newPagePath = createPage(table);
                    Vector<Hashtable<String,Object>> newPageVector = readVectorFromPageFile(newPagePath);
                    insertToPageAndShift(mainPageVector,newPageVector,colNameValue,table);
                    writeVectorToPageFile(mainPageVector);
                    writeVectorToPageFile(newPageVector);

               }
           }


       }

   }
    //returns overflow record(from second page) if the page we shift to is ful
    //returns null if there is no overflow from the second page
    private Hashtable<String,Object> insertToPageAndShift(Vector<Hashtable<String, Object>> toInsertIn,Vector<Hashtable<String, Object>> toShiftTo,Hashtable<String,Object> colNameValue,Table table){
        String clusteringColumn = table.getClusteringColumn();
        Hashtable<String,Object> overflowRecord = null;
        insertToVector(toInsertIn,colNameValue,clusteringColumn);
        overflowRecord = toInsertIn.lastElement();
        toInsertIn.remove(overflowRecord);
        insertToVector(toShiftTo,colNameValue,clusteringColumn);
        Hashtable<String,Object> overflowFromSecondPage = null;
        if (checkIfPageisMoreThanFull(toShiftTo)) {
            overflowFromSecondPage = toShiftTo.lastElement();
            toShiftTo.remove(overflowFromSecondPage);
        }
        modifyHeaderInsert(toInsertIn,table);
        modifyHeaderInsert(toShiftTo,table);

        return overflowFromSecondPage;
    }

//   //returns overflow record(from second page) if the page we shift to is ful
//   //returns null if there is no overflow from the second page
//   private Hashtable<String,Object> insertToPageAndShift(Vector<Hashtable<String, Object>> toInsertIn,Vector<Hashtable<String, Object>> toShiftTo,Hashtable<String,Object> colNameValue,Table table){
//        String clusteringColumn = table.getClusteringColumn();
//        Hashtable<String,Object> overflowRecord = null;
//        insertToVector(toInsertIn,colNameValue,clusteringColumn);
//        overflowRecord = toInsertIn.lastElement();
//        toInsertIn.remove(overflowRecord);
//        insertToVector(toShiftTo,colNameValue,clusteringColumn);
//        Hashtable<String,Object> overflowFromSecondPage = null;
//       if (checkIfPageisMoreThanFull(toShiftTo)) {
//           overflowFromSecondPage = toShiftTo.lastElement();
//           toShiftTo.remove(overflowFromSecondPage);
//       }
//       modifyHeaderInsert(toInsertIn,table);
//       modifyHeaderInsert(toShiftTo,table);
//
//       return overflowFromSecondPage;
//   }

//    private void insertToPage(Vector<Hashtable<String, Object>> mainPageVector,Hashtable<String,Object> colNameValue, Table table) throws IOException {
//    }
    public static boolean checkInputs (String tableName,Hashtable <String,Object> colNameValue) throws IOException, ParseException {
        Vector<String[]> data=null;
        String row;
        File csvFile = new File(metadataCSVPath);
        if (csvFile.isFile())
        {
            BufferedReader csvReader = new BufferedReader(new FileReader(metadataCSVPath));
            while (( row = csvReader.readLine()) != null) {
                String[] temp = row.split(",");
                 data.add(temp);
            }
            csvReader.close();
            boolean primaryExists=false;
            Set<String> keys = colNameValue.keySet();
            String[]temp2=null;
            for(String key: keys){
                for (int i = 0; i <data.size()-1 ; i++) {
                    temp2=data.get(i);
                    if(temp2[3].equals("true"))
                    {
                        primaryExists=true;
                    }
                    String type=temp2[2];


                    if(temp2[0].equals(tableName)&&temp2[1].equals(key))
                    {
                        switch (type){
                            case "Integer":if(!(colNameValue.get(key) instanceof Integer)||((Integer) colNameValue.get(key)).compareTo(Integer.parseInt(temp2[5]))<0||((Integer) colNameValue.get(key)).compareTo(Integer.parseInt(temp2[6]))>0)
                                return false;
                            case "Date":{
                                SimpleDateFormat sdFormat = new SimpleDateFormat("yyyy-mm-dd");
                                String max=temp2[6];
                                String min=temp2[5];
                                Date d =(Date)colNameValue.get(key);
                                DateFormat dateFormat=new SimpleDateFormat("yyyy-mm-dd");
                                String str= dateFormat.format(d);
                                Date dmin=new SimpleDateFormat("yyyy-mm-dd").parse(min);
                                Date dmax=new SimpleDateFormat("yyyy-mm-dd").parse(max);
                                Date d1= new SimpleDateFormat(("yyyy-mm-dd")).parse(str);
                                if(!(colNameValue.get(key) instanceof Date)||(d1.compareTo(dmin))<0||d1.compareTo(dmax)>0)

                                return false;
                            }
                            case "String":if(!(colNameValue.get(key) instanceof String)||((String) colNameValue.get(key)).compareTo(temp2[5])<0||((String) colNameValue.get(key)).compareTo(temp2[6])>0)
                                return false;
                            case "Double":if(!(colNameValue.get(key) instanceof Double)||((Double) colNameValue.get(key)).compareTo(Double.parseDouble(temp2[5]))<0||((Double) colNameValue.get(key)).compareTo(Double.parseDouble(temp2[6]))>0)
                                return false;
                            default:return false;

                        }
                    }


                }

            }
            if(primaryExists=false)
                return false;
            return true;
        }
     return false;
    }

    private void insertToPage(Vector<Hashtable<String, Object>> mainPageVector, Comparable insertKey,Hashtable<String,Object> colNameValue, Table table) throws IOException {
        String clusteringColumn = table.getClusteringColumn();
        insertToVector(mainPageVector,colNameValue,clusteringColumn);
        modifyHeaderInsert(mainPageVector,table);
        writeVectorToPageFile(mainPageVector);

    }

    private static String getPageNameFromHeader(Vector<Hashtable<String, Object>> mainPageVector) {
        Hashtable<String,Object> header = mainPageVector.get(0);
        return (String) header.get("pageName");
    }

    private static void insertToVector(Vector<Hashtable<String, Object>> mainPageVector, Hashtable<String, Object> colNameValue, String clusteringColumn) {

        Comparable insertKey =(Comparable) colNameValue.get(clusteringColumn);
        int indexToInsertIn =  binarySearchForInsertIndex(mainPageVector,insertKey,clusteringColumn);

        //if Hashtable  should  be inserted at the end
        if(indexToInsertIn>=mainPageVector.size())
        {
            mainPageVector.add(colNameValue);
            return;
            return;
        }
        //object at the index that we should insert at
        Hashtable<String,Object> recordToInsertAt = mainPageVector.get(indexToInsertIn);
        Comparable recordKey =(Comparable) recordToInsertAt.get(clusteringColumn);

        if(insertKey.compareTo(recordKey)>0)
        {
            insertAfterIndex(mainPageVector,colNameValue,indexToInsertIn);
        }
        else {
            insertBeforeIndex(mainPageVector,colNameValue,indexToInsertIn);
        }

    }

    private static void insertBeforeIndex(Vector<Hashtable<String, Object>> mainPageVector, Hashtable<String, Object> colNameValue, int indexToInsertIn) {
        mainPageVector.insertElementAt(colNameValue,indexToInsertIn);

    }

    private static void insertAfterIndex(Vector<Hashtable<String, Object>> mainPageVector, Hashtable<String, Object> colNameValue, int indexToInsertIn) {
        indexToInsertIn++;
        mainPageVector.insertElementAt(colNameValue,indexToInsertIn);
    }

    private static ArrayList<Vector<Hashtable<String,Object>>> getOverflowPages(Vector<Hashtable<String, Object>> mainPageVector) throws IOException, ClassNotFoundException {
        ArrayList<Vector<Hashtable<String,Object>>> overFlowPages = new ArrayList<>();
        Vector<Hashtable<String, Object>> overFlowPageVector = null;
        Vector<Hashtable<String, Object>> loopPageVector = mainPageVector;
        Hashtable<String,Object> loopPageHeader= null;
        String overFlowPageName = null;

         while(true) {
             loopPageHeader = loopPageVector.get(0);
             overFlowPageName = (String) loopPageHeader.get("overflowPageName");
             if (!overFlowPageName.equals(""))
             {
                 String overFlowPagePath = getPagePath(overFlowPageName);
                 overFlowPageVector = readVectorFromPageFile(overFlowPagePath);
                 overFlowPages.add(overFlowPageVector);
                 loopPageVector = overFlowPageVector;
             }
             else
             {
                 break;
             }
         }



        return overFlowPages;
    }

    private static boolean checkIfPageIsFull(Vector<Hashtable<String, Object>> mainPage) {
        return mainPage.size()>=maxRows+1;
    }
    private static boolean checkIfPageisMoreThanFull(Vector<Hashtable<String, Object>> mainPage){
        return mainPage.size()>maxRows+1;
    }

    public void createTable(String strTableName,
                            String strClusteringKeyColumn, Hashtable<String,String> htblColNameType,
                            Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax )
            throws DBAppException, IOException {
        //check if table name is unique

        writeCsvTable( strTableName,strClusteringKeyColumn, htblColNameType,htblColNameMin, htblColNameMax);
        intializeTables();

        intializeTables();

    }



    public void createIndex(String strTableName,String[] strarrColName)throws DBAppException{


    }


    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException, IOException, ClassNotFoundException, ParseException {
        Table table= getTable(tableName);
        if(checkInputs(tableName,columnNameValue)==false){
            throw new DBAppException();
        }
        //when trying to insert into a table that doesn't exist
        if (table  == null)
            throw new DBAppException();

        ArrayList<String>  pages = table.getPages();
        String clusteringColumn = table.getClusteringColumn();

    if(pages.isEmpty()){
          throw new DBAppException();}

            ArrayList<Vector> pagesToSearchIn = getPageToSearchIn(clusteringKeyValue,pages,table);


            if(pagesToSearchIn.size()==0)
                throw new DBAppException();

            Vector<Hashtable<String,Object>> mainPageVector = pagesToSearchIn.get(0);
            ArrayList<Vector<Hashtable<String,Object>>> overFlowPagesOfMainPage = getOverflowPages(mainPageVector);

            //First search in main page vector
            if(binarySearchForKey(mainPageVector,clusteringKeyValue,clusteringColumn)!=-1)
            {
                mainPageVector.set(binarySearchForKey(mainPageVector,clusteringKeyValue,clusteringColumn),columnNameValue);
                return;
            }
            //if not found search in overflow pages
            for (int i=0;i< overFlowPagesOfMainPage.size();i++){
                if(binarySearchForKey(overFlowPagesOfMainPage.get(i),clusteringKeyValue,clusteringColumn)!=-1){
                    overFlowPagesOfMainPage.get(i).set(binarySearchForKey(overFlowPagesOfMainPage.get(i),clusteringKeyValue,clusteringColumn),columnNameValue);
                    return;
                }
            }
        }



    //empty page -> delete it
    // empty page with overflow -> shift

    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException, IOException, ClassNotFoundException {
        Table table = getTable(tableName);
        if(table==null){
            throw new DBAppException();
        }
        ArrayList<String> pages= table.getPages();

        //correct or doesn't count overflow?
        if(pages.isEmpty()){
            throw new DBAppException();
        }
        //looping on pages
        for (int i = 0; i <pages.size() ; i++) {
            String pagePath=getPagePath(pages.get(i));

            Vector<Hashtable<String,Object>> pageVector =  readVectorFromPageFile(pagePath);

            ArrayList<Vector<Hashtable<String,Object>>>overflowPages=getOverflowPages(pageVector);

            deleteAllRecordsFromPage(pageVector,columnNameValue);
            deleteAllRecordsFromOverflowPages(overflowPages,columnNameValue);

            if(checkIfPageIsEmpty(pageVector)){
                deletePage(pages.get(i),table);
            }
            //need to check on empty overflow










        }

    }


    public static boolean checkIfDelete(Hashtable<String,Object> record,Hashtable<String,Object>deleteCondition){


        return false;
    }
    public static void deleteRecord(Vector<Hashtable<String,Object>> pageVector,Hashtable<String,Object> record) {
        pageVector.remove(record);
    }

    public static void deleteEmptyPage(Vector<Hashtable<String,Object>> pageVector,ArrayList<Vector<Hashtable<String,Object>>> overFlowPages){
        //switch vectors and change header
        // switch overflowpages
        if(checkIfPageIsEmpty(pageVector)){

        }
    }

    public static void deletePage(String pageName,Table table) throws IOException, ClassNotFoundException {
        String pagePath=getPagePath(pageName);
        Path path= FileSystems.getDefault().getPath(pagePath);
        Vector<Hashtable<String,Object>> pageVector =  readVectorFromPageFile(pagePath);

        ArrayList<Vector<Hashtable<String,Object>>>overflowPages=getOverflowPages(pageVector);

        try {
            Files.deleteIfExists(path);
        } catch (NoSuchFileException x) {
            System.err.format("%s: no such" + " file or directory%n", pagePath);
        } catch (DirectoryNotEmptyException x) {
            System.err.format("%s not empty%n", pagePath);
        } catch (IOException x) {
            System.err.println(x);
        }

        table.removePage(pageName);

    }



    public static void deleteAllRecordsFromPage(Vector<Hashtable<String,Object>>pageVector,Hashtable<String,Object>deleteCondition){
        for (int i = 1; i <pageVector.size() ; i++) {
            Hashtable<String,Object> record = pageVector.get(i);
            if(checkIfDelete(record,deleteCondition)){
                deleteRecord(pageVector,record);
            }

        }
    }

    public static void deleteAllRecordsFromOverflowPages(ArrayList<Vector<Hashtable<String,Object>>>overFlowArray,Hashtable<String,Object>deleteCondition){
        for (int i = 0; i <overFlowArray.size() ; i++) {
            Vector<Hashtable<String, Object>> overflowPageVector = overFlowArray.get(i);
            deleteAllRecordsFromPage(overflowPageVector,deleteCondition);
        }
    }

    public static boolean checkIfPageIsEmpty(Vector<Hashtable<String,Object>> pageVector){
        if(pageVector.size()==1){
            return true;
        }
        return false;
    }










    public static void modifyHeaderdelete(Vector<Hashtable<String,Object>> pageVector,Table table) {
        Hashtable<String, Object> header = (Hashtable<String, Object>) pageVector.get(0);
        String clusteringColumn = table.getClusteringColumn();
        Comparable maxKeyHeader = (Comparable) header.get("maxKey");
        Comparable minKeyHeader = (Comparable) header.get("minKey");
        Comparable maxKey = (Comparable) pageVector.lastElement().get(clusteringColumn);
        Comparable minKey = (Comparable) pageVector.get(1).get(clusteringColumn);

        if ( maxKeyHeader==null || minKeyHeader==null ){
            header.put("maxKey",maxKey);
            header.put("minKey",minKey);
        }
        else
        {
            if(maxKeyHeader.compareTo(maxKey)<0){
                header.put("maxKey",maxKey);
            }
            if(minKeyHeader.compareTo(minKey)>0){
                header.put("minKey",minKey);
            }
        }
    }

    public static void  deletePage(Table table,String pagePath) throws IOException {





    }




    public Iterator  selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException{

       return null;
    }


    private static void setConfig() {
        Properties prop = new Properties();
        String fileName = "src/main/resources/DBApp.config";
        InputStream is = null;
        try {
            is = new FileInputStream(fileName);
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }
        try {
            prop.load(is);
        } catch (IOException ex) {
            ex.printStackTrace();
        }

        maxRows=Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
        maxIndexBucket=Integer.parseInt(prop.getProperty("MaximumKeysCountinIndexBucket"));

    }


   private boolean pagesContainKey(ArrayList<Vector<Hashtable<String,Object>>> pages,Comparable key,String clusteringColumn){
       for (Vector<Hashtable<String, Object>> page :
               pages) {
           if (binarySearchForKey(page,key,clusteringColumn)!= -1)
               return true;
       }
       return false;
   }
    

//    private boolean  pageContainKey(Vector<Hashtable<String,Object>> pageVector,Comparable key,String clusteringColumn){
//        for (Hashtable<String,Object> record :
//               pageVector ) {
//            Comparable recordKey =(Comparable) record.get(clusteringColumn);
//            if (recordKey.compareTo(key)==0){
//                return true;
//            }
//
//        }
//        return false;
//    }

    void writeCsvTable(String strTableName,
                       String strClusteringKeyColumn, Hashtable<String,String> htblColNameType,
                       Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax ) throws IOException
    {

        //getting keys
        Enumeration<String> colNameEnumration = htblColNameType.keys();


        while(colNameEnumration.hasMoreElements()){


            String colName = colNameEnumration.nextElement();
            String colType = htblColNameType.get(colName);
            Comparable colMin =  htblColNameMin.get(colName);
            Comparable colMax =htblColNameMax.get(colName);

            boolean isClusterKey  =colName.equals(strClusteringKeyColumn);
            boolean indexed = false;

            writeCsvColumn(strTableName,colName,colType,isClusterKey,indexed,colMin,colMax);


        }

    }

    void writeCsvColumn(String tableName,String columnName,String columnType,
                        boolean clusteringKey,boolean indexed,Comparable min,Comparable max )throws IOException
    {
        FileWriter csvWriter = new FileWriter(metadataCSVPath,true);
        String minString = min.toString();
        String maxString = min.toString();

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

        csvWriter.append(",");
        csvWriter.append(min.toString());
        csvWriter.append(",");
        csvWriter.append(max.toString());

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
                    if(tableCSV[ClUSTERING_KEY].equals("TRUE")){
                        table.setClusteringColumn(tableCSV[COLUMN_NAME]);
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


//    // if return -1 then all pages are full
//    //otherwise returns the number of the first not full page
//    public static boolean checkPagesIsNotFull(Table table) throws IOException, ClassNotFoundException {
//        ArrayList<String> pages = table.getPages();
//
//        for (int  i =0;i<pages.size();i++)
//        {
//          String page = pages.get(i);
//          String pagePath = getPagePath(page);
//          boolean pageIsFull =  readVectorFromPageFile(pagePath).size()>=maxRows ;
//          if(!pageIsFull){
//              return true;
//          }
//
//        }
//
//
//        return false;
//    }


    public static String getPageName(String tableName,int pageNumber){
        return tableName +""+pageNumber+".class";
    }

    public static void insertIntoEmptyTable(Table table, Hashtable<String,Object> colNameValue) throws IOException, ClassNotFoundException {
        String newPagePath = createPage(table);
        Vector<Hashtable<String,Object>> pageVector =  readVectorFromPageFile(newPagePath);
        pageVector.add(colNameValue);
        modifyHeaderInsert(pageVector,table);
        writeVectorToPageFile(pageVector);
    }

    public static void modifyMainPageVectorHeadeAfterInsertOverflow(Vector<Hashtable<String,Object>> pageVector,Hashtable<String,Object> colNameValue,Table table)
    {
        Hashtable<String,Object> header = (Hashtable<String, Object>) pageVector.get(0);
        String clusteringColumn  = table.getClusteringColumn();
        Comparable maxKeyHeader = (Comparable) header.get("maxKey");
        Comparable minKeyHeader = (Comparable) header.get("minKey");
        Comparable insertKey = (Comparable) colNameValue.get(clusteringColumn);
            if(maxKeyHeader.compareTo(insertKey)<0){
                header.replace("maxKey",insertKey);
            }
            if(minKeyHeader.compareTo(insertKey)>0){
                header.replace("minKey",insertKey);
            }
    }

    //takes pageVector and and record to be inserted and modify the header
    public static void modifyHeaderInsert(Vector<Hashtable<String,Object>> pageVector,Table table){
        Hashtable<String,Object> header = (Hashtable<String, Object>) pageVector.get(0);
        String clusteringColumn  = table.getClusteringColumn();
        Comparable maxKeyHeader = (Comparable) header.get("maxKey");
        Comparable minKeyHeader = (Comparable) header.get("minKey");
        Comparable maxKey = (Comparable) pageVector.lastElement().get(clusteringColumn);
        Comparable minKey = (Comparable) pageVector.get(1).get(clusteringColumn);





        if ( maxKeyHeader==null || minKeyHeader==null ){
            header.put("maxKey",maxKey);
            header.put("minKey",minKey);
        }
        else
        {
            if(maxKeyHeader.compareTo(maxKey)<0){
                    header.replace("maxKey",maxKey);
            }
            if(minKeyHeader.compareTo(minKey)>0){
                header.replace("minKey",minKey);
            }
        }





    }


    //check if page is related to the table by the naming convention
    public static boolean checkPageTable(Table table,String pageName)
    {
        String  tableName = table.getName();
        return (pageName.startsWith(tableName) && !pageName.contains("_overflow")) ;
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
            String pageName = getPageName(table.getName(), pageNumber);
            createPageFile(pagePath);
            //create Vector and write  it to the page
            Vector<Hashtable<String,Object> > pageVector = new Vector<Hashtable<String,Object>>();
            createHeaderForNewPage(pageVector,pageName);
            table.addPage(getPageName(table.getName(),pageNumber));
            writeVectorToPageFile(pageVector);

            return  pagePath;
        }

        public static String createOverflowPage(Table table,Vector<Hashtable<String,Object>> parentPage) throws IOException {
          Hashtable<String,Object> parentPageHeader = parentPage.get(0);
          String parentIsOverflow = (String) parentPageHeader.get("isOverflowOf");
          String parentPageName = (String) parentPageHeader.get("pageName");
          String overflowPageName ;
          String overflowPagePath ;
          int overFlowNumber = 0;
          //if the parent page is not overflow
          if(parentIsOverflow == null)
          {
            overFlowNumber=0;
          }
          else{
              int parentOverflowNumver = getOverFlowNumber(parentPageName);
              overFlowNumber=parentOverflowNumver+1;
          }

          overflowPageName=getOverflowPageName(parentPageName,overFlowNumber);
          overflowPagePath=getPagePath(overflowPageName);
          createPageFile(overflowPagePath);
          Vector<Hashtable<String,Object>> overflowPageVector = new Vector<>();
          createHeaderForOverflowPage(overflowPageVector,parentPageName,overflowPageName);
          writeVectorToPageFile(overflowPageVector);
          return overflowPagePath;
        }

    private static String getOverflowPageName(String parentPageName, int overFlowNumber) {
        String parentPageNameNoClass = parentPageName.replaceAll(".class","");
//        System.out.println(parentPageNameNoClass);
        return parentPageNameNoClass+"_overflow_"+overFlowNumber+".class";
    }

    private static int getOverFlowNumber(String parentPageName) {
        int overflowNumber=0;
        //.class length is 6
        int indexOfOverflowNumber = parentPageName.length()-7;
        String[] splitted = parentPageName.split("_");
        // [overflowNumber].class
        String overFlowDotClass = splitted[splitted.length-1];
        // overflowNumber string
        String[] temp = overFlowDotClass.split(".class");
        String overflowNumberString = temp[0];
        overflowNumber = Integer.parseInt(overflowNumberString);
        return overflowNumber;
    }


    public static void createHeaderForNewPage(Vector<Hashtable<String,Object>> pageVector ,String pageName){
        Hashtable<String,Object> header =  new Hashtable<String,Object>();
        header.put("overflowPageName","");
//        header.put("maxKey","");
//        header.put("minKey","");
        header.put("isOverFlowOf","");
        header.put("pageName",pageName);
        pageVector.insertElementAt(header,0);
        }

        // returns vector of page and  the next page
        // if it returns Arraylist of size 1 then the page returned is the last page
        public static ArrayList<Vector> getPageToInsertIn(Hashtable<String,Object>  colNameValue ,ArrayList<String>  pages ,Table table) throws IOException, ClassNotFoundException {

        ArrayList<Vector> retArray =  new ArrayList<>();
        for (int i = 0;i<pages.size();i++) {
                String page = pages.get(i);
                String pagePath=getPagePath(page);
                Vector<Hashtable<String,Object>> pageVector= readVectorFromPageFile(pagePath);

                if(i==0 && !checkKeyGreaterThanMin(pageVector,colNameValue,table) )
                {

                    retArray.add(pageVector);
                    checkIfNextPageExistsAndAdd(i,pages,retArray);
                    break;
                }

                if(checkKeyGreaterThanMin(pageVector,colNameValue,table)){
                    retArray.removeAll(retArray);
                    retArray.add(pageVector);
                    checkIfNextPageExistsAndAdd(i,pages,retArray);

                }
            }

        //check if exists next page and add it s
        return retArray;

        }

        public static void checkIfNextPageExistsAndAdd(int i,ArrayList<String> pages,ArrayList<Vector> retArray) throws IOException, ClassNotFoundException {
            i++;
            if(i<pages.size())
            {
                String page = pages.get(i);
                String pagePath=getPagePath(page);
                Vector<Hashtable<String,Object>> nextPageVector = readVectorFromPageFile(pagePath);
                retArray.add(nextPageVector);
            }

        }

        //return if key is greater than minKey
        public static boolean checkKeyGreaterThanMin(Vector<Hashtable<String,Object>> pageVector,Hashtable<String,Object> colNameValue,Table table){

            String clusteringColumn =  table.getClusteringColumn();
            Hashtable<String,Object> header = pageVector.get(0);
            Comparable clusteringKey =(Comparable) colNameValue.get(clusteringColumn);
            Comparable minKey = (Comparable) header.get("minKey");
            if (minKey==null)
            {
                return true;
            }
            boolean retBoolean = (minKey.compareTo(clusteringKey)<=0 );
            return retBoolean;

        }



    public static void createHeaderForOverflowPage(Vector overflowPage,String parentPageName,String pageName){
            Hashtable<String,Object> header =  new Hashtable<String,Object>();
            header.put("overflowPageName","");
//            header.put("maxKey","");
//            header.put("minKey","");
            header.put("isOverFlowOf",parentPageName);
            header.put("pageName",pageName);
            overflowPage.add(header);


        }

        public static void createPageFile(String pagePath) throws IOException {


            File pageFile = new File(pagePath);
            pageFile.createNewFile();

        }



        public static void writeVectorToPageFile(Vector<Hashtable<String,Object>> vector) throws IOException {
            String pageName = getPageNameFromHeader(vector);
            String pagePath = getPagePath(pageName);
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

    public static int binarySearchForKey (Vector<Hashtable<String,Object>> vector, Comparable searchKey,String clustrColumn){
        return binarySearchForKey(vector,searchKey,1,vector.size(),clustrColumn);
    }


    public static int binarySearchForKey(Vector<Hashtable<String,Object>> vector, Comparable searchKey, int low, int high, String clustrColumn) {
        while (low <= high)
        {
            int mid = (low + high)/2;
            Comparable midKey =(Comparable) vector.get(mid).get(clustrColumn);
            if (searchKey.compareTo(midKey)==0) {
                return mid;
            }
            else if (searchKey.compareTo(midKey)<0) {
                high = mid - 1;
            }
            else {
                low = mid + 1;
            }
        }

        // target doesn't exist in the array
        return -1;
    }
    public static int binarySearchForInsertIndex(Vector<Hashtable<String,Object>> vector, Comparable insertKey, String clustrColumn) {
      return  binarySearchForInsertIndex(vector,insertKey,1,vector.size(),clustrColumn);
    }


    // returns the index that we should insert in
    // we must check if the index is greater than the vector size

        public static int binarySearchForInsertIndex(Vector<Hashtable<String,Object>> vector, Comparable insertKey, int low, int high, String clustrColumn) {
        int index = Integer.MAX_VALUE;
//            Comparable minKey;x
//         if(vector.isEmpty())
//             minKey= (Comparable) vector.get(0).get(clustrColumn);
//
//            if(insertKey<)
        while (low != high && low+1!=high) {
            int mid = (low + high) / 2;
            Comparable midKey =(Comparable) vector.get(mid).get(clustrColumn);
            if (midKey.compareTo(insertKey)<0){
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

            if(low+1==high)
            {
                return low+1;
            }
            else
                return low;
    }

    public static ArrayList<Vector> getPageToSearchIn(String clusteringKeyValue ,ArrayList<String>  pages ,Table table) throws IOException, ClassNotFoundException {

        ArrayList<Vector> retArray =  new ArrayList<>();
        for (int i = 0;i<pages.size();i++) {
            String page = pages.get(i);
            String pagePath=getPagePath(page);
            Vector<Hashtable<String,Object>> pageVector= readVectorFromPageFile(pagePath);

            if(i==0 && !checkKeyGreaterThanMin(pageVector,clusteringKeyValue,table) )
            {

                retArray.add(pageVector);

                break;
            }

            if(checkKeyGreaterThanMin(pageVector,clusteringKeyValue,table)){
                retArray.add(pageVector);

                break;
            }
        }

        //check if exists next page and add it s
        return retArray;

    }
    public static boolean checkKeyGreaterThanMin(Vector<Hashtable<String,Object>> pageVector,String clusteringKeyValue,Table table){

        String clusteringColumn =  table.getClusteringColumn();
        Hashtable<String,Object> header = pageVector.get(0);
        Comparable clusteringKey =(Comparable) clusteringKeyValue;
        Comparable minKey = (Comparable) header.get("minKey");
        if(minKey==null) // || minkey.equals("")
            return true;
        boolean retBoolean = (minKey.compareTo(clusteringKey)<=0 );
        return retBoolean;

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
//        Hashtable<String,Object> testHash = new Hashtable<>();
//        testHash.put("id",1);
//        testHash.put("name","Ahmed");
//        testHash.put("gpa","5.0");
//        insertIntoEmptyTable(test,testHash);
//        Vector<Hashtable<String,Object>> vector = readVectorFromPageFile(getPagePath("Student",0));


//        Hashtable<String,Object> h1 = new Hashtable<>();
//        h1.put("id",0);
//        Hashtable<String,Object> h2 = new Hashtable<>();
//        h2.put("id",2);
//        Hashtable<String,Object> h3 = new Hashtable<>();
//        h3.put("id",3);
//        Hashtable<String,Object> h4 = new Hashtable<>();
//        h4.put("id",4);
//        Hashtable<String,Object> h5 = new Hashtable<>();
//        h5.put("id",5);
//        Hashtable<String,Object> h6 = new Hashtable<>();
//        h6.put("id",6);
//
//        Vector<Hashtable<String,Object>> vector = new Vector<>();
//        vector.add(h1);
//        vector.add(h2);
//        vector.add(h3);
//        vector.add(h4);
//        vector.add(h5);
//        vector.add(h6);

       // System.out.print(binarySearchForInsertIndex(vector,7,0,vector.size(),"id"));
//
//        String pageName = "Table0.class";
//        System.out.print(getOverflowPageName(pageName,12));
//        setConfig();
//        Hashtable htblColNameType = new Hashtable( );
//        htblColNameType.put("id", "java.lang.Integer");
//
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
//        dbApp.init();
//        Table test = getTable(strTableName);
//        String page0Path = createPage(test);
//        Vector<Hashtable<String,Object>> page0 =readVectorFromPageFile(page0Path);
//        int i =0;
//        while(!checkIfPageIsFull(page0)) {
//            Hashtable<String,Object> col = new Hashtable<>();
//            col.put("id",i);
//            insertToVector(page0, col, "id");
//            i++;
//        }
//        modifyHeaderInsert(page0,test);
//        writeVectorToPageFile(page0);
//        String page1Path = createPage(test);
//        Vector<Hashtable<String,Object>> page1 =readVectorFromPageFile(page1Path);
//        while(!checkIfPageIsFull(page1)) {
//            Hashtable<String,Object> col = new Hashtable<>();
//            col.put("id",i);
//            insertToVector(page1, col, "id");
//            i++;
//        }
//        modifyHeaderInsert(page1,test);
//        writeVectorToPageFile(page1);
//        Hashtable<String,Object> col1= new Hashtable<>();
//        col1.put("id",600);
//
//    ArrayList<Vector> rt =    getPageToInsertIn(col1,test.getPages(),test);
//
//
//

    }

}
