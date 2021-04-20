import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;

public class DBApp implements DBAppInterface{

   public void init(){

    }

   public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException{

    }

   public void createTable(String strTableName,
                            String strClusteringKeyColumn, Hashtable<String,String> htblColNameType,
                            Hashtable<String,String> htblColNameMin, Hashtable<String,String> htblColNameMax )
            throws DBAppException, IOException {
        writeCsvTable( strTableName,
                 strClusteringKeyColumn, htblColNameType,htblColNameMin, htblColNameMax);
    }



    public void createIndex(String strTableName,String[] strarrColName)throws DBAppException{


    }


    public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> columnNameValue) throws DBAppException
    {

    }


    public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException{

    }

    public Iterator  selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException{

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
        FileWriter csvWriter = new FileWriter("new.csv");

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

        csvWriter.append("/n");


        csvWriter.flush();
        csvWriter.close();



    }


    public static void main(String[] args)throws DBAppException,IOException {
        String strTableName = "Student"; DBApp dbApp = new DBApp( );
        Hashtable htblColNameType = new Hashtable( ); htblColNameType.put("id", "java.lang.Integer");
        htblColNameType.put("name", "java.lang.String");
        htblColNameType.put("gpa", "java.lang.double");
        Hashtable<String,String> htblColNameMin = new Hashtable( );
        Hashtable<String,String> htblColNameMax = new Hashtable( );
        htblColNameMin.put("name","0");
        htblColNameMin.put("gpa","0");
        htblColNameMin.put("id","0");

        htblColNameMax.put("name","0");
        htblColNameMax.put("gpa","4");
        htblColNameMax.put("id","313242");




        dbApp.createTable( strTableName, "id", htblColNameType,htblColNameMin,htblColNameMax );
    }

}
