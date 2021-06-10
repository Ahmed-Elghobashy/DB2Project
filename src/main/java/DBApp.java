import java.io.*;
import java.nio.file.*;
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

    //naming convention for index :Index_[tablename]_[ColNamesindexed].class
    //example : Index_Student_GPA_ID.class ;

    //naming convention for overflow bucket : [tablename]Bucket[bucketNum]_overflow_[overflow number].class
    //naming convenvtion for bucket: [tablename]Bucket[BucketNum]__[ColNamesindexed].class
    //     example : StudentBucket0.class ;

    //bucket has header
    // Hashtable :: Key:overflowBucketName , Value : "[].class"
    //              key:overflowOf   ,Value: ".class"
    //              key:bucketName ,     Value: "[].class"


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

    //GridIndex

    //to do M2
    // Fix overflow errors /optimize overflow
    // index in intitalizeTables
    // max min in Table insertion/deleteting/updating
    // intialzie ranges in grid index
    //




    //does the header count in the max rows ?



    //naming conventionforOverFlowPages  [tablename][page_number]_overflow_[overflow number].class

    private static  final String metadataCSVPath = "src/main/resources/metadata.csv" ;
    private static  final String pagesDirectoryPath = "src/main/resources/data" ;
    private static int maxRows ;
    private static int maxIndexBucket;
     static final String dateType="java.util.Date";
     static final String stringType="java.lang.String";
     static final String intType="java.lang.Integer";
     static final String doubleType="java.lang.Double";


    private static final ArrayList<Table> tables = new ArrayList<Table>();
    private static final Vector<GridIndex> indices = new Vector<>();

    public static void writeIndexToFile(GridIndex index) {
        String indexFileName = index.getFileName();
        String indexPath = pagesDirectoryPath + "/" + indexFileName;
        try {
            createPageFile(indexPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            FileOutputStream fileOut = new FileOutputStream(indexPath);
            ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
            objectOut.writeObject(index);
            objectOut.close();
        } catch (IOException e) {
            e.printStackTrace();


        }
    }

    public static GridIndex readIndexFromFile(String indexPath)
    {
        FileInputStream fileIn = null;
        try {
            fileIn = new FileInputStream(indexPath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        ObjectInputStream objectIn = null;
        try {
            objectIn = new ObjectInputStream(fileIn);
        } catch (IOException e) {
            e.printStackTrace();
        }
        GridIndex retIndex = null;
        try {
            retIndex = (GridIndex) objectIn.readObject();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return retIndex;

    }


    public void init()  {


        try {
            intializeTables();
            intializIndices();
        } catch (IOException e) {
            e.printStackTrace();
        }
        setConfig();
    }

    private void intializIndices() {
        String[] pages = listPages();

        for (String page :
                pages) {
            if (page.contains("index"))
                intializeIndex(page);
        }
    }

    private void intializeIndex(String indexName) {
        String indexPath = pagesDirectoryPath+"/"+indexName;
        GridIndex index = readIndexFromFile(indexPath);
        indices.add(index);
    }


    public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException, IOException, ClassNotFoundException {


        Table table= getTable(tableName);

       if(checkInputsMod(table,colNameValue)==false){
           throw new DBAppException();
       }


       //when trying to insert into a table that doesn't exist
        if (table  == null)
            return;

        ArrayList<String>  pages = table.getPages();
        String clusteringColumn = table.getClusteringColumn();
        Comparable insertKey =(Comparable) colNameValue.get(clusteringColumn);
        //check for errors in input


       //       if(checkInputs(tableName,colNameValue)==false){
//           throw new DBAppException();
//       }



       //if no pages exist(The table is empty), Create page and insert
       if(pages.isEmpty()){
        insertIntoEmptyTable(table,colNameValue);
        return;
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
               writeVectorToPageFile(mainPageVector);

               return;


           }
           else {
               //else if  the page is  full we check if it has overflowPages
               if(overFlowPagesOfMainPage.size()>1)
               {
                 Vector<Hashtable<String,Object>> overflowPageVector = overFlowPagesOfMainPage.get(0);
                 if(!checkIfPageIsFull(overflowPageVector)){
                     insertToPage(overflowPageVector,colNameValue,table);
                     modifyMainPageVectorHeadeAfterInsertOverflow(mainPageVector,colNameValue,table);
                     writeVectorToPageFile(overflowPageVector);
                     writeVectorToPageFile(mainPageVector);
                     return;
                 }
                 else{
                     Hashtable<String,Object> tempOverflowRecord = colNameValue;

                     for (int i = 0; i <overFlowPagesOfMainPage.size() ; i++) {
                         Vector<Hashtable<String,Object>> currentPageVector = overFlowPagesOfMainPage.get(i);
                        if(i==overFlowPagesOfMainPage.size()-1){
                            //create new overflow page and shift to it
                            Vector<Hashtable<String,Object>> newOverflowVector = createOverflowPage(table,currentPageVector);
                            insertToPageAndShift(currentPageVector,newOverflowVector,colNameValue,table);
                            writeVectorToPageFile(currentPageVector);
                            writeVectorToPageFile(newOverflowVector);
                            return;
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
                       Vector<Hashtable<String,Object>> overflowPageVector = createOverflowPage(table,mainPageVector);
                       //
                        //insert to overflow page
                       insertToVector(overflowPageVector,colNameValue,clusteringColumn);
                       insertRecordToIndices(colNameValue,overflowPageVector,table);
                       modifyHeader(overflowPageVector,table);
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
                    Vector<Hashtable<String,Object>> newPageVector = createPage(table);
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
        //Index ----- -&&&&&& ------
        insertRecordToIndices(colNameValue,toInsertIn,table);
        //&&&&&&&&&&&&&&&&&&&&&&&&&&&
        overflowRecord = toInsertIn.lastElement();
        toInsertIn.remove(overflowRecord);
        insertToVector(toShiftTo,overflowRecord,clusteringColumn);
        //Index ----- -&&&&&& ------
        //REMOVE FROM INDEX
        insertRecordToIndices(overflowRecord,toShiftTo,table);
        //&&&&&&&&&&&&&&&&&&&&&&&&&&&

        Hashtable<String,Object> overflowFromSecondPage = null;
        if (checkIfPageisMoreThanFull(toShiftTo)) {
            overflowFromSecondPage = toShiftTo.lastElement();
            toShiftTo.remove(overflowFromSecondPage);
            //REMOVE FROM INDEX
        }
        modifyHeader(toInsertIn,table);
        modifyHeader(toShiftTo,table);

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
        Vector<String[]> data=new Vector<>();
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


    public static boolean checkInputsMod (Table table,Hashtable <String,Object> colNameValue)  {



        Set<String> columnNamesInput = colNameValue.keySet();
        Iterator<String> itr = columnNamesInput.iterator();
        while (itr.hasNext())
        {
            String colName = itr.next();
            if(!columnNamesInput.contains(colName))
                return false;
            String colType = table.getColumnType(colName);
            Comparable colMin = table.getColumnMin(colName);
            Comparable colMax = table.getColumnMax(colName);
            Comparable colValue = (Comparable) colNameValue.get(colName);

            // if trying to insert a column that doesn't exist
            if(colMax==null || colMin==null || colType==null)
                return false;

            boolean checkType = checkInputType(colValue,colType);
            if(checkType==false)
            {
                return false;
            }
            boolean checkMin = checkInputMin(colValue,colMin);
            boolean checkMax = checkInputMax(colValue,colMax);

            if (!checkMax || !checkMin)
                return false;
        }
        

        return true;
    }

    private static boolean checkInputMax(Comparable colValue, Comparable colMax) {
        return colValue.compareTo(colMax)<=0;

    }

    private static boolean checkInputMin(Comparable colValue, Comparable colMin) {
        return colValue.compareTo(colMin)>=0;
    }

    private static boolean checkInputType(Comparable colValue, String colType) {

        if(colValue instanceof Date && colType.equals(dateType))
            return true;
        if(colValue instanceof Integer && colType.equals(intType))
            return true;
        if(colValue instanceof Double && colType.equals(doubleType))
            return true;
        if(colValue instanceof String && colType.equals(stringType))
            return true;

        return false;


    }

    private void insertToPage(Vector<Hashtable<String, Object>> mainPageVector,Hashtable<String,Object> colNameValue, Table table) throws IOException {
        String clusteringColumn = table.getClusteringColumn();
        insertToVector(mainPageVector,colNameValue,clusteringColumn);
        modifyHeader(mainPageVector,table);
        insertRecordToIndices(colNameValue,mainPageVector,table);
//        writeVectorToPageFile(mainPageVector);

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

    public static ArrayList<Vector<Hashtable<String,Object>>> getOverflowPages(Vector<Hashtable<String, Object>> mainPageVector)  {
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
        Table table =getTable(strTableName);
        GridIndex index = new GridIndex(table,strarrColName);
        indices.add(index);

    }


    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException {
        Table table= getTable(tableName);
        if(checkInputsMod(table,columnNameValue)==false){
            throw new DBAppException();
        }
        //when trying to insert into a table that doesn't exist
        if (table  == null)
            throw new DBAppException();

        ArrayList<String>  pages = table.getPages();
        String clusteringColumn = table.getClusteringColumn();
        String clusteringKeyType = table.getColumnType(clusteringColumn);
        Comparable clusteringKey =castTo(clusteringKeyValue,clusteringKeyType);
        columnNameValue.put(clusteringColumn,clusteringKeyValue);


        if(pages.isEmpty()){
          throw new DBAppException();}

            ArrayList<Vector> pagesToSearchIn = getPageToSearchIn(clusteringKeyValue,pages,table);


            if(pagesToSearchIn.size()==0)
                throw new DBAppException();

            Vector<Hashtable<String,Object>> mainPageVector = pagesToSearchIn.get(0);
            ArrayList<Vector<Hashtable<String,Object>>> overFlowPagesOfMainPage = getOverflowPages(mainPageVector);


            int indexToinsertIn = binarySearchForKey(mainPageVector,clusteringKey,clusteringColumn);
        //First search in main page vector
            if(indexToinsertIn!=-1)
            {
                mainPageVector.set(indexToinsertIn,columnNameValue);
                writeVectorToPageFile(mainPageVector);
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



    /* check if modifyheader is correct
    * check if pagevector=overflow.get(0) is correct
    *
    *
    *
    *
    *
    * */

    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
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
        //delete from all pages if found

            deleteAllRecordsFromPage(pageVector,columnNameValue,table);
            deleteAllRecordsFromOverflowPages(overflowPages,columnNameValue,table);
            deleteOverflowPages(overflowPages,table);


            if(checkIfPageIsEmpty(pageVector)&&checkIfPageHasNoOverflow(pageVector)){
                    deletePage(pages.get(i),table);
            }

            if(checkIfPageIsEmpty(pageVector)&&!checkIfPageHasNoOverflow(pageVector)){
                switchMainPageWithOverflow(pageVector,overflowPages,table);
            }








        }

    }

    private void switchOverflowToEmptyPage(Vector<Hashtable<String, Object>> pageVector, ArrayList<Vector<Hashtable<String, Object>>> overflowPages) {
        for (int i = 0; i < overflowPages.size(); i++) {
            if(overflowPages.get(i).size()>1){

            }

        }
    }

    private boolean checkIfPageHasNoOverflow(Vector<Hashtable<String, Object>> pageVector)  {
        ArrayList<Vector<Hashtable<String,Object>>> overflowPages = getOverflowPages(pageVector);

        return overflowPages.size()==0;

    }


    public static boolean checkIfDelete(Hashtable<String,Object> record,Hashtable<String,Object>deleteCondition){

        Set<String> deleteConditionKeys = deleteCondition.keySet();
        Iterator<String> i = deleteConditionKeys.iterator();
        while(i.hasNext())
        {
            String deleteConditionKey = i.next();
            Comparable deleteConditionValue = (Comparable) deleteCondition.get(deleteConditionKey);
            Comparable recordValue = (Comparable) record.get(deleteConditionKey);

            if (!recordValue.equals(deleteConditionValue))
                return false;

        }
        return true;
    }

    public static void deleteRecord(Vector<Hashtable<String,Object>> pageVector,Hashtable<String,Object> record,Table table)  {
        pageVector.remove(record);
        modifyHeader(pageVector,table);
    }



    public static void deletePage(String pageName,Table table)  {
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





    public static void deleteAllRecordsFromPage(Vector<Hashtable<String,Object>>pageVector,Hashtable<String,Object>deleteCondition,Table table)  {
        for (int i = 1; i <pageVector.size() ; i++) {
            Hashtable<String,Object> record = pageVector.get(i);
            if(checkIfDelete(record,deleteCondition)){
                deleteRecord(pageVector,record,table);
                i--;
            }

        }
        writeVectorToPageFile(pageVector);
    }

    public static void deleteAllRecordsFromOverflowPages(ArrayList<Vector<Hashtable<String,Object>>>overFlowArray,Hashtable<String,Object>deleteCondition,Table table)  {
        for (int i = 0; i <overFlowArray.size() ; i++) {
            Vector<Hashtable<String, Object>> overflowPageVector = overFlowArray.get(i);
            deleteAllRecordsFromPage(overflowPageVector,deleteCondition,table);
            writeVectorToPageFile(overflowPageVector);
        }

    }

    public static boolean checkIfPageIsEmpty(Vector<Hashtable<String,Object>> pageVector){
        if(pageVector.size()==1){
            return true;
        }
        return false;
    }



    public static void deleteOverflowPages(ArrayList<Vector<Hashtable<String,Object>>> overflowPages,Table table)  {



        for (int i = 0; i < overflowPages.size(); i++) {
            if(checkIfPageIsEmpty(overflowPages.get(i))){
                modifyHeaderThenDeleteOverflowPage(overflowPages.get(i),table);
                overflowPages.remove(i);
                i--;
            }
        }
    }





    public static void modifyHeaderThenDeleteOverflowPage(Vector<Hashtable<String,Object>> overflowVector,Table table)  {
        Hashtable<String, Object> header = (Hashtable<String, Object>) overflowVector.get(0);
        String pageName = (String) header.get("pageName");
        String previousPage = (String) header.get("isOverflowOf");
        String nextPage = (String) header.get("overflowPageName");

        String previousPagePath=getPagePath(previousPage);
        String nextPagePath = getPagePath(nextPage);

        Vector<Hashtable<String,Object>> previousPageVector= readVectorFromPageFile(previousPagePath);
        Vector<Hashtable<String,Object>> nextPagePathVector= readVectorFromPageFile(nextPagePath);

        Hashtable<String, Object> previousPageHeader = (Hashtable<String, Object>) previousPageVector.get(0);
        Hashtable<String, Object> nextPageHeader = (Hashtable<String, Object>) nextPagePathVector.get(0);

        nextPageHeader.put("isOverFlowOf",previousPage);
        previousPageHeader.replace("overflowPageName",nextPage);

        deletePage(pageName,table);


    }






        public static void switchMainPageWithOverflow(Vector<Hashtable<String,Object>> pageVector,ArrayList<Vector<Hashtable<String,Object>>> overflowPages,Table table)  {
        //check if this is correct
        Vector<Hashtable<String, Object>> overflowPage = overflowPages.get(0);
        moveAllRecords(overflowPage,pageVector);
        modifyHeaderThenDeleteOverflowPage(overflowPage,table);

    }
    public static void moveAllRecords(Vector<Hashtable<String,Object>> moveFrom, Vector<Hashtable<String, Object>> moveTo){
        for (int i = 0; i < moveFrom.size(); i++) {
            Hashtable<String,Object> record = moveFrom.get(i);
            moveTo.add(record);
            
        }

    }

  



    public static void modifyHeaderdelete(Vector<Hashtable<String,Object>> pageVector,Table table) throws IOException, ClassNotFoundException {
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

    public static boolean operatorChecker(String operator,Comparable record,Comparable compareValue){
        switch(operator){
            case "=": return record.equals(compareValue);
            case "!=": return !record.equals(compareValue );
            case ">": return(record.compareTo(compareValue)>0); //record>compareValue
            case ">=": return(record.compareTo(compareValue)>=0);
            case "<": return(record.compareTo(compareValue)<0);
            case "<=": return(record.compareTo(compareValue)<=0);
            default: return false;
        }
    }

    public static boolean conditionCheck(Hashtable<String,Object> record,String columnName,Object value,String operator){
            Comparable recordValue = (Comparable) record.get(columnName);
            if (operatorChecker(operator,recordValue, (Comparable) value)){
                return true;
            }
        return false;
        }


    public static ArrayList<Hashtable<String, Object>> searchAllRecordsFromPage(Vector<Hashtable<String,Object>>pageVector,String columnName,Object value,String operator) {
        ArrayList<Hashtable<String, Object>> records=new ArrayList<Hashtable<String, Object>>();
        for (int i = 1; i < pageVector.size(); i++) {
            Hashtable<String,Object> record=pageVector.get(i);
            if(conditionCheck(record,columnName,value,operator)){
                records.add(record);
            }

        }
        return records;
    }
        public static ArrayList<Hashtable<String, Object>> searchAllRecordsFromOverflowPages(ArrayList<Vector<Hashtable<String,Object>>>overFlowArray,String columnName,Object value,String operator)  {
        ArrayList<Hashtable<String, Object>> records = new ArrayList<Hashtable<String, Object>>();
        for (int i = 0; i <overFlowArray.size() ; i++) {
                Vector<Hashtable<String, Object>> overflowPageVector = overFlowArray.get(i);
            for (int j = 1; j < overflowPageVector.size(); j++) {
                Hashtable<String, Object> record = overflowPageVector.get(i);
                if (conditionCheck(record, columnName, value,operator)) {
                    records.add(record);
                }
            }
            }
        return records;

        }

    public GridIndex getIndexOfColumns(String tableName, Vector<String>columnNames){
        Vector<GridIndex> tableIndices = getTableIndices(tableName);
        for (GridIndex index :
                tableIndices) {
            if (isIndexOfColumns(index, columnNames)) {
                return index;
            }
        }

        return null;
    }

    private boolean isIndexOfColumns(GridIndex index, Vector<String> columnNames) {
        String[] columnsIndexed=index.getColumnsIndexed();
        boolean columnFound=false;
        for (String column :
                columnsIndexed) {
            for(int i=0;i< columnNames.size();i++){
                if(column.equals(columnNames.get(i))){
                    columnNames.remove(i);
                    columnFound=true;
                    break;
                }
            }
            if(columnFound==false){
                return false;
            }
            columnFound=false;
        }
        if(columnNames.isEmpty()){
            return true;
        }
        else {
            return false;
        }
        }
        public Vector<String> getTermsColumns(SQLTerm[] sqlTerms){
        Vector<String> columns=new Vector<>();
        for(int i=0;i<sqlTerms.length;i++){
            columns.add(sqlTerms[i].columnName);
        }
        return columns;
        }
    public Vector<GridIndex> getTableIndices(String tableName){
        GridIndex index=null;
        Vector<GridIndex> tableIndices=new Vector<GridIndex>();
        for(int i=0;i<indices.size();i++){
            index=indices.get(i);
            if(index.table.getName().equals(tableName)){
            tableIndices.add(index);
            }
        }
        return tableIndices;
    }
    public ArrayList<Hashtable<String, Object>> selectFromIndexedTable(SQLTerm sqlTerm,String[] arrayOperators)throws DBAppException{

        return null;
    }
    public ArrayList<Hashtable<String, Object>> selectFromNonIndexedTable(SQLTerm sqlTerm,String[] arrayOperators)throws DBAppException{
        Table table = getTable(sqlTerm.tableName);

        if(table==null) {
            throw new DBAppException();
        }
        ArrayList<String>pages = table.getPages();
        if(pages.isEmpty()){
            throw new DBAppException();
        }
        ArrayList<Hashtable<String, Object>>records=new ArrayList<Hashtable<String, Object>>();
        for (int i = 0; i <pages.size() ; i++) {
            String pagePath = getPagePath(pages.get(i));

            Vector<Hashtable<String, Object>> pageVector = readVectorFromPageFile(pagePath);
            ArrayList<Vector<Hashtable<String, Object>>> overflowPages = getOverflowPages(pageVector);
            records.addAll(searchAllRecordsFromPage(pageVector,sqlTerm.columnName, sqlTerm.objValue,sqlTerm.operator));
            records.addAll(searchAllRecordsFromOverflowPages(overflowPages, sqlTerm.columnName, sqlTerm.objValue,sqlTerm.operator));
        }
        return records;
    }
    public ArrayList<Hashtable<String, Object>> arrayOperatorGenerator(String operator,ArrayList<Hashtable<String, Object>>record,ArrayList<Hashtable<String, Object>>temp,String clusteringColumn){
        boolean Flag=false;
        ArrayList<Hashtable<String, Object>> returnArray=new ArrayList<Hashtable<String, Object>>();
        if(operator.equals("OR")) {
            for (int i = 0; i <temp.size(); i++) {
                for(int j=0;j<record.size();j++){
     if(temp.get(i).get(clusteringColumn).equals(record.get(j).get(clusteringColumn))){
         Flag=true;
     }
             }
                if(Flag==false){
                    record.add(temp.get(i));
                }
                Flag=false;
            }
            return record;
        }
        if(operator.equals("AND")){

            for (int i = 0; i <temp.size(); i++) {
                for(int j=0;j<record.size();j++){
                    if(temp.get(i).get(clusteringColumn).equals(record.get(j).get(clusteringColumn))){
                        Flag=true;
                    }
                }
                if(Flag==true){
                    returnArray.add(temp.get(i));
                }
                Flag=false;
            }
            return returnArray;
        }
        if(operator.equals("XOR")){
            for (int i = 0; i <temp.size(); i++) {
                for(int j=0;j<record.size();j++){
                    if(temp.get(i).get(clusteringColumn).equals(record.get(j).get(clusteringColumn))){
                        Flag=true;
                    }
                }
                if(Flag==false){
                    returnArray.add(temp.get(i));
                }
                Flag=false;
            }
            for (int i = 0; i <record.size(); i++) {
                for(int j=0;j<temp.size();j++){
                    if(temp.get(i).get(clusteringColumn).equals(record.get(j).get(clusteringColumn))){
                        Flag=true;
                    }
                }
                if(Flag==false){
                    returnArray.add(temp.get(i));
                }
                Flag=false;
            }
            return returnArray;
        }
        return null;
        }


    public Iterator  selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException{
        ArrayList<Hashtable<String, Object>> records = new ArrayList<Hashtable<String, Object>>();
        ArrayList<Hashtable<String, Object>> temp= new ArrayList<Hashtable<String, Object>>();
        boolean Indexed=false;
        String tableName=sqlTerms[0].tableName;
        Vector<String> selectionColumns = getTermsColumns(sqlTerms);
        GridIndex index=getIndexOfColumns(tableName,selectionColumns);
        if(index==null) {
            for (int i = 0; i < sqlTerms.length; i++) {
                if (i == 0) {
                    temp=selectFromNonIndexedTable(sqlTerms[i], arrayOperators);
                    records.addAll(temp);
                } else {
                    Table table = getTable(sqlTerms[i].tableName);
                    String clusteringColumn = table.getClusteringColumn();
                    temp = (selectFromNonIndexedTable(sqlTerms[i], arrayOperators));
                    records = (arrayOperatorGenerator(arrayOperators[i], records, temp, clusteringColumn));
                }
            }
        }
        else{
            
        }
       Iterator iterator=records.iterator();
       return iterator;
    }
    //------------------------------------------------------------------------------------------------------
    public Vector<Vector<Integer>> bucketIndices(GridIndex index,Vector<Vector<Integer>> columnIndices,String arrOperator){
        Vector<IndexRange[]> ranges=index.getRangesColumns();
        int column;
        int dimension=columnIndices.size();
        Vector<Integer> bucketIndex=new Vector<>(dimension);
        Vector<Vector<Integer>> bucketIndices=new Vector<>();
        switch(arrOperator){
            case "OR":{
                for(int i=0;i<columnIndices.size();i++){
                    for (int j = 0; j < columnIndices.get(i).size() ; j++){
                       column= columnIndices.get(i).get(j);
                       for(int k=0;)
                    }
                }
            }

        }
        return bucketIndices;
    }
    public static Vector<Vector<Integer>> columnIndices(GridIndex index,SQLTerm sqlTerms[]){
        Vector<Vector<Integer>> columnIndices=new Vector<>();
        Vector<IndexRange[]> ranges=index.getRangesColumns();
        for(int i=0; i<sqlTerms.length;i++){
            String column=sqlTerms[i].columnName;
            for(int j=0;j<ranges.size();j++){
                if((ranges.get(j))[0].columnName.equals(column)){
                    columnIndices.add(bucketChecker(sqlTerms[i].operator,ranges.get(j),(Comparable) sqlTerms[i].objValue));
                    break;
                }
            }
        }
        return columnIndices;
    }
    public static Vector<Integer> bucketChecker(String operator,IndexRange[] ranges,Comparable compareValue){
        Vector<Integer> buckets=new Vector<>();
        switch(operator){
            case "=": {
                for(int i=0;i<ranges.length;i++){
                    if(ranges[i].isInRange(compareValue)){
                        buckets.add(i);
                        break;
                    }
                }
                return buckets;
            }
            case "!=": {
                for(int i=0;i<ranges.length;i++){
                    if(!ranges[i].isInRange(compareValue)){
                        buckets.add(i);
                    }
                }
                return buckets;
            }
            case ">": {
                for(int i=0;i<ranges.length;i++){
                    if(ranges[i].min.compareTo(compareValue)>=0||ranges[i].max.compareTo(compareValue)<0){
                        buckets.add(i);
                    }
                }
                return buckets;
            } //min>compareValue
            case ">=":{
                for(int i=0;i<ranges.length;i++){
                if(ranges[i].min.compareTo(compareValue)>=0||ranges[i].max.compareTo(compareValue)<=0){
                    buckets.add(i);
                }
            }
                return buckets;
        }
            case "<":{
                for(int i=0;i<ranges.length;i++){
                if(ranges[i].min.compareTo(compareValue)<0||ranges[i].max.compareTo(compareValue)>=0){
                    buckets.add(i);
                }
            }
                return buckets;
        }
            case "<=": {
                for(int i=0;i<ranges.length;i++){
                    if(ranges[i].min.compareTo(compareValue)<=0||ranges[i].max.compareTo(compareValue)>=0){
                        buckets.add(i);
                    }
                }
                return buckets;
            }
            default: return buckets;
        }
    }
//------------------------------------------------------------------------------------------------------------


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
                String colType = tableCSV[COLUMN_TYPE];


                if(tableCSV[TABLE_NAME].equals(tableName))
                {
                    Hashtable<String,String> column = new Hashtable<>();
                    column.put(tableCSV[COLUMN_NAME],tableCSV[COLUMN_TYPE]);
                    //set columns type
                    ArrayList< Hashtable<String,String>> columns = table.getColumnsType();
                    columns.add(column);
                    //set clustering key
                    if(tableCSV[ClUSTERING_KEY].equals("TRUE")){
                        table.setClusteringColumn(tableCSV[COLUMN_NAME]);
                    }
                    //set min
                    ArrayList<Hashtable<String,Comparable>> colMin = table.getColumnsMin();
                    Hashtable<String,Comparable> min = new Hashtable<>();
                    String minString = tableCSV[MIN];
                    Comparable minValue = castTo(minString,colType);
                    min.put(tableCSV[COLUMN_NAME],minValue);
                    colMin.add(min);
                    //set max
                    ArrayList<Hashtable<String,Comparable>> colMax = table.getColumnsMax();
                    Hashtable<String,Comparable> max = new Hashtable<>();
                    String maxString = tableCSV[MAX];
                    Comparable maxValue = castTo(maxString,colType);
                    max.put(tableCSV[COLUMN_NAME],maxValue);
                    colMax.add(max);





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


    public static void insertRecordToIndices(Hashtable<String,Object> record , Vector pageVector,Table table)
    {
        Vector<GridIndex> tableIndices = getTableIndices(table);

        for (GridIndex index :
                tableIndices) {
            insertRecordToIndex(record,pageVector,index);
        }
        
    }

    private static Vector<GridIndex> getTableIndices(Table table) {

        Vector<GridIndex> tableIndices = new Vector<>();
        for (GridIndex index :
                indices) {
            if(isIndexOfTable(index,table))
                tableIndices.add(index);
        }

        return tableIndices;


    }

    private static boolean isIndexOfTable(GridIndex index, Table table) {

        return index.getTable().getName().equals(table.getName());
    }

    public static void insertRecordToIndex(Hashtable<String,Object> record , Vector pageVector,GridIndex index)
    {
        String pageName =getPageNameFromHeader(pageVector);
        index.inesrtRecordToIndex(record,pageName);
    }

    public static String getPageName(String tableName,int pageNumber){
        return tableName +""+pageNumber+".class";
    }

    public static void insertIntoEmptyTable(Table table, Hashtable<String,Object> colNameValue) throws IOException, ClassNotFoundException {
        Vector<Hashtable<String,Object>> pageVector = createPage(table);

        pageVector.add(colNameValue);
        modifyHeader(pageVector,table);
        writeVectorToPageFile(pageVector);
        insertRecordToIndices(colNameValue,pageVector,table);
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

    //takes pageVector and  and modify the header
    public static void modifyHeader(Vector<Hashtable<String,Object>> pageVector, Table table){
        Hashtable<String,Object> header = (Hashtable<String, Object>) pageVector.get(0);
        if(pageVector.size()==1){
            return;
        }
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
        return (pageName.startsWith(tableName) && !pageName.contains("_overflow")) &&!pageName.contains("Bucket") &&!pageName.contains("Index") ;
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



      public static Table getTable(String tableName){

            for (int i = 0; i <tables.size() ; i++) {
                if(tables.get(i).getName().equals(tableName))
                {
                    return  tables.get(i);
                }

            }
            return null;
        }

             static Vector createBucket(GridIndex index)
        {
            String bucketName=getBucketName(index);
            String bucketPath = getBucketPath(bucketName);
            Vector<Object> bucket=null;
            try {
                createPageFile(bucketPath);
                bucket = new Vector<>();
                createBucketHeader(bucketName,bucket);
                writeBucketVectorToFile(bucket);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return bucket;
        }


    private static String getBucketName(GridIndex index) {
        Table table =index.getTable();
        String tableName= table.getName();
        int bucketNum= index.getBucketsNum();

        return tableName+"Bucket"+bucketNum+".class";
    }

    static String getBucketPath(String bucketName){

        return pagesDirectoryPath +"/"+bucketName;
    }

    static Hashtable createBucketHeader(String bucketName,Vector bucketVector) {
        Hashtable<String,String> header = new Hashtable<>();
        header.put("bucketName",bucketName);
        bucketVector.add(header);
        return header;

    }


        static Vector<Object> createOverflowBucket(GridIndex index, Vector parentBucket){

            String overflowBucketName = getOverflowBucketName(parentBucket);
            String parentBucketName = getBucketNameFromHeader(parentBucket);
            Vector overflowBucket = null;

            try {
                createPageFile(overflowBucketName);
                overflowBucket=new Vector();
                createHeaderForOverflowBucket(overflowBucket,overflowBucketName,parentBucketName);
                modifyParentBucketHeaderAfterAddingOverflow(overflowBucketName,parentBucket);
                writeBucketVectorToFile(parentBucket);
                writeBucketVectorToFile(overflowBucket);

                //addBucketToIndex(overflowBucket,index);

            } catch (IOException e) {
                e.printStackTrace();
            }

            return overflowBucket;
        }

     static String getBucketNameFromHeader(Vector bucket) {
        Hashtable<String,String> header = (Hashtable<String, String>) bucket.get(0);


        return header.get("bucketName");
    }

     static void modifyParentBucketHeaderAfterAddingOverflow(String overflowBucketName,Vector parentBucket) {
        Hashtable<String,String> header = (Hashtable<String, String>) parentBucket.get(0);

        header.put("overflowBucketName",overflowBucketName);

    }

     static void createHeaderForOverflowBucket(Vector overflowBucket,String overflowBucketName, String parentBucketName) {
        Hashtable<String,String> header = new Hashtable<>();
        header.put("bucketName",overflowBucketName);
        header.put("overflowOf",parentBucketName);
        overflowBucket.add(header);


    }

     static String getOverflowBucketName(Vector parentBucket) {
        Hashtable<String,String> parentBucketHeader =(Hashtable<String, String>) parentBucket.get(0);
        String parentBucketName = parentBucketHeader.get("bucketName");
        int overflowBucketNumber;

        if(parentBucketHeader.get("overflowOf")==null)
            overflowBucketNumber=0;
        else
        overflowBucketNumber= getOverFlowNumber(parentBucketName)+1;
        String parentBucketNameWithoutClass = parentBucketName.split(".class")[0];

        return parentBucketNameWithoutClass+"_"+"overflow"+overflowBucketNumber+".class";

    }


    //return page vector
        public static Vector  createPage(Table table) throws IOException {

            int pageNumber = table.getPages().size();
            String pagePath = getPagePath(table.getName(),pageNumber);
            String pageName = getPageName(table.getName(), pageNumber);
            createPageFile(pagePath);
            //create Vector and write  it to the page
            Vector<Hashtable<String,Object> > pageVector = new Vector<Hashtable<String,Object>>();
            createHeaderForNewPage(pageVector,pageName);
            table.addPage(getPageName(table.getName(),pageNumber));
            writeVectorToPageFile(pageVector);

            return  pageVector;
        }

        public static Vector createOverflowPage(Table table,Vector<Hashtable<String,Object>> parentPage) throws IOException {
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
              int parentOverflowNumber = getOverFlowNumber(parentPageName);
              overFlowNumber=parentOverflowNumber+1;
          }
          overflowPageName=getOverflowPageName(parentPageName,overFlowNumber);
          overflowPagePath=getPagePath(overflowPageName);
          createPageFile(overflowPagePath);
          Vector<Hashtable<String,Object>> overflowPageVector = new Vector<>();
          createHeaderForOverflowPage(overflowPageVector,parentPageName,overflowPageName);
          writeVectorToPageFile(overflowPageVector);
          parentPageHeader.replace("overflowPageName",overflowPageName);
          writeVectorToPageFile(parentPage);
            return overflowPageVector;
        }

    private static String getOverflowPageName(String parentPageName, int overFlowNumber) {
        String parentPageNameNoClass = parentPageName.replaceAll(".class","");
//        System.out.println(parentPageNameNoClass);
        return parentPageNameNoClass+"_overflow_"+overFlowNumber+".class";
    }

    private static int getOverFlowNumber(String parentPageName) {
        int overflowNumber=0;
        //.class length is 6
//        int indexOfOverflowNumber = parentPageName.length()-7;
        String[] splitted = parentPageName.split("_");
        // [overflowNumber].class
        String overFlowDotClass = splitted[splitted.length-1];
        // overflowNumber string
        String[] temp = overFlowDotClass.split(".class");
        String overflowNumberString = temp[0];
        overflowNumber = Integer.parseInt(overflowNumberString);
        return overflowNumber;
    }

    private static int getOverFlowNumberMod(String parentPageName) {
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



        public static void writeVectorToPageFile(Vector vector)  {
            String pageName = getPageNameFromHeader(vector);
            String pagePath = getPagePath(pageName);
            writeVectorToFile(vector,pagePath);

        }


        static void writeVectorToFile(Vector vector,String pagePath)
        {
            try {
                FileOutputStream fileOut = new FileOutputStream(pagePath);
                ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
                objectOut.writeObject(vector);
                objectOut.close();
            }
            catch (IOException e){
                e.printStackTrace();
            }
        }

        public static String getPagePath(String tableName,int pageNumber){

        return pagesDirectoryPath+"/"+tableName+""+pageNumber+".class";
        }

        public static  String getPagePath (String pageName){
        return  pagesDirectoryPath+"/"+pageName ;
        }


        public static Vector readVectorFromPageFile(String pagePath) {
            FileInputStream fileIn = null;
            try {
                fileIn = new FileInputStream(pagePath);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            ObjectInputStream objectIn = null;
            try {
                objectIn = new ObjectInputStream(fileIn);
            } catch (IOException e) {
                e.printStackTrace();
            }
            Vector retVector = null;
            try {
                retVector = (Vector) objectIn.readObject();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return retVector;
        }

    public static int binarySearchForKey (Vector<Hashtable<String,Object>> vector, Comparable searchKey,String clustrColumn){
        return binarySearchForKey(vector,searchKey,1,vector.size()-1,clustrColumn);
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

    public static ArrayList<Vector> getPageToSearchIn(String clusteringKeyValue ,ArrayList<String>  pages ,Table table)  {

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
        String clusteringKeyType = table.getColumnType(clusteringColumn);
        Comparable clusteringKey =castTo(clusteringKeyValue,clusteringKeyType);


        //clustering key value cast to comparable other than string

        Comparable minKey = (Comparable) header.get("minKey");
        if(minKey==null) // || minkey.equals("")
            return true;
        boolean retBoolean = (minKey.compareTo(clusteringKey)<=0 );
        return retBoolean;

    }

    public static Comparable castTo (String valueString,String type)
    {
        Comparable ret=valueString ;
        if(type.equals(intType))
        {
            ret=Integer.parseInt(valueString);
        }
        else if(type.equals(doubleType)){
            ret=Double.parseDouble(valueString);
        }
        else if(type.equals(dateType))
        {
            ret=formatDate(valueString);
        }

        return ret;
    }

//    public static boolean inDateFormat(String valueString){
//
//        return false;
//    }

    public static Date formatDate(String dateString)
    {
        String[] dateStrArr = dateString.split("-");
        int year = Integer.parseInt(dateStrArr[0]);
        int month = Integer.parseInt(dateStrArr[1]);
        int day =  Integer.parseInt(dateStrArr[2]);

        Date date = new Date(year-1900,month-1,day);

        return date;
    }

    public static void insertToBucket(Vector<Object> bucket, BucketIndex bucketIndex,GridIndex index) {
        if(bucketIsFull(bucket))
        {
            Vector<Object> overflowBucketToInsertin = getOverflowBucketToInsertIn(bucket,index);
            overflowBucketToInsertin.add(bucketIndex);
            writeVectorToPageFile(overflowBucketToInsertin);
        }
        else{
            bucket.add(bucketIndex);
            writeBucketVectorToFile(bucket);
        }
    }

    private static Vector<Object> getOverflowBucketToInsertIn(Vector<Object> bucket,GridIndex index) {
        Vector<Vector<Object>> overflowBuckets = getOveflowBuckets(bucket);
        for (Vector<Object> tmpBucket :
                overflowBuckets) {
            if(!bucketIsFull(tmpBucket))
                return tmpBucket;

        }
        // if there are no non full overflows create one

        Vector<Object> parentBucket = overflowBuckets.lastElement();
        Vector<Object> retBucket = createOverflowBucket(index,parentBucket);

        return retBucket;

    }

    private static Vector<Vector<Object>> getOveflowBuckets(Vector<Object> bucket) {
        Vector<Vector<Object>> overflowBuckets = new Vector<>();
        Vector<Object> loopBucket = bucket;
        while(hasOverflowBucket(loopBucket))
        {
            Vector overflowBucket = getOverflowBucket(bucket);
            overflowBuckets.add(overflowBucket);
            loopBucket=overflowBucket;
        }


        return overflowBuckets;
    }

    private static Vector getOverflowBucket(Vector<Object> bucket) {
        Hashtable<String,String> header = (Hashtable<String, String>) bucket.get(0);
        String overflowBucketName =header.get("overflowBucketName");

        String bucketName = getBucketNameFromHeader(bucket);
        String bucketPath= getBucketPath(bucketName);

        Vector overflowBucket = readVectorFromPageFile(bucketPath);


        return overflowBucket;
    }

    private static boolean hasOverflowBucket(Vector<Object> bucket) {
        Hashtable<String,String> header = (Hashtable<String, String>) bucket.get(0);
        String overflowBucketName =header.get("overflowBucketName");

        if (overflowBucketName==null)
            return false;
        else
            return true;
    }

    private static void writeBucketVectorToFile(Vector<Object> bucket) {
        String bucketName =getBucketNameFromHeader(bucket);
        String bucketPath = getBucketPath(bucketName);

        writeVectorToFile(bucket,bucketPath);
    }

    private static boolean bucketIsFull(Vector<Object> bucket) {

        return bucket.size()>= maxIndexBucket+1;
    }

    public static boolean checkIfIndexed(String tableName ,Hashtable<String,Object> checkConditions){

        for (int i = 0; i <indices.size() ; i++) {
            GridIndex temp=indices.get(i);
            if(temp.getTable().getName().equals(tableName)){
                String[] tempInd = temp.columnsIndexed;
                for (int j = 0; j <tempInd.length ; j++) {
                    Set<String> keys=checkConditions.keySet();
                    for (String key: keys){
                        if(key.equals(tempInd[j]))
                            return true;
                    }

                }
            }

        }
        return false;
    }
//    public static void insertToIndex(Hashtable<String,Object> record,GridIndex index){
//
//        //
//
//    }

    public static void main(String[] args) throws DBAppException, IOException, ClassNotFoundException {
        String strTableName = "Student";
        DBApp dbApp = new DBApp( );
        dbApp.init();

        Hashtable htblColNameType = new Hashtable( ); htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.Double");
        Hashtable<String,String> htblColNameMin = new Hashtable( );
        Hashtable<String,String> htblColNameMax = new Hashtable( );
        htblColNameMin.put("name","A");
        htblColNameMin.put("gpa","0");
        htblColNameMin.put("id","0");

        htblColNameMax.put("name","AAAAAAAA");
        htblColNameMax.put("gpa","4");
        htblColNameMax.put("id","313242");
//
//
//
//
       dbApp.createTable( strTableName, "id", htblColNameType,htblColNameMin,htblColNameMax );
//
        Hashtable<String,Object> testHash = new Hashtable<>();
        testHash.put("id",1);
        testHash.put("name","AA");
        testHash.put("gpa",2.0);
        Hashtable<String,Object> testHash2 = new Hashtable<>();
        testHash2.put("id",2);
        testHash2.put("name","A");
        testHash2.put("gpa",2.0);
        Hashtable<String,Object> testHash3 = new Hashtable<>();
        testHash3.put("id",3);
        testHash3.put("name","AA");
        testHash3.put("gpa",2.0);
        SQLTerm[] arrSQLTerms;
        arrSQLTerms = new SQLTerm[2];
        arrSQLTerms[0]=new SQLTerm();
        arrSQLTerms[0].tableName = "Student";
        arrSQLTerms[0].columnName= "name";
        arrSQLTerms[0].operator = "=";
        arrSQLTerms[0].objValue = "AA";
        arrSQLTerms[1]=new SQLTerm();
        arrSQLTerms[1].tableName = "Student";
        arrSQLTerms[1].columnName= "gpa";
        arrSQLTerms[1].operator = "=";
        arrSQLTerms[1].objValue = new Double( 2.0 );

        dbApp.insertIntoTable(strTableName,testHash2);
        dbApp.insertIntoTable(strTableName,testHash);
        dbApp.insertIntoTable(strTableName,testHash3);
        String[]strarrOperators = new String[1];
        strarrOperators[0] = "OR";
// select * from Student where name = “John Noor” or gpa = 1.5;
        Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);

  //      dbApp.createIndex(strTableName,new String[]{"gpa","name"});
 //       GridIndex index = readIndexFromFile(pagesDirectoryPath+"/StudentIndex_gpa_name.class");
//        dbApp.insertIntoTable(str);
////          dbApp.init();
//        Vector v =readVectorFromPageFile(getPagePath("Student0.class"));
//        Hashtable<String,Object> testHash1 = new Hashtable<>();
////        testHash1.put("id",1);
//        testHash1.put("name","AA");
////        testHash1.put("gpa",3.0);
//        dbApp.deleteFromTable(strTableName,testHash1);
//        v =readVectorFromPageFile(getPagePath("Student0.class"));
//
//
//        insertIntoEmptyTable(test,testHash);
//        Vector<Hashtable<String,Object>> vector = readVectorFromPageFile(getPagePath("Student",0));
//
//
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
//
//        System.out.print(binarySearchForInsertIndex(vector,7,0,vector.size(),"id"));
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

//        Vector v = readVectorFromPageFile(getPagePath("transcripts0_overflow_0.class"));
//        ArrayList m = getOverflowPages(v);


    }

}
