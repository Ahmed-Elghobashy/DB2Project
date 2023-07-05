# DB2Project

## Description

The Mini Database Engine is a lightweight Java library that provides basic functionality for managing tables, pages, and buckets in a mini database system. It offers features for creating, reading, updating, and deleting data in a simple and efficient manner.


## Methods

The class includes the following methods:

### checkPageTable

```java
public static boolean checkPageTable(Table table, String pageName)
```

This method checks if a given page name belongs to the specified table. It ensures that the page name starts with the table name, does not contain "_overflow," "Bucket," or "Index" keywords.

### listPages

```java
public static String[] listPages()
```

This method retrieves a list of page names from the pages directory and returns them in sorted order.

### readCSV

```java
public ArrayList<String[]> readCSV(String path) throws IOException
```

This method reads a CSV file and returns its content as a list of string arrays, where each array represents a row in the CSV file.

### getTable

```java
public static Table getTable(String tableName)
```

This method retrieves a table object based on the given table name from a list of tables.

### createBucket

```java
static Vector createBucket(GridIndex index)
```

This method creates a new bucket for a given grid index. It generates a bucket name, creates the corresponding page file, initializes the bucket vector, creates and adds a bucket header, and writes the bucket vector to the file.

### getBucketName

```java
private static String getBucketName(GridIndex index)
```

This method generates a bucket name based on the given grid index and table name.

### getBucketPath

```java
static String getBucketPath(String bucketName)
```

This method returns the full path of a bucket file based on the given bucket name.

### createBucketHeader

```java
static Hashtable createBucketHeader(String bucketName, Vector bucketVector)
```

This method creates a header for a bucket, associating the bucket name with the bucket vector. It returns the created header.

### createOverflowBucket

```java
static Vector<Object> createOverflowBucket(GridIndex index, Vector parentBucket)
```

This method creates an overflow bucket for a given grid index and parent bucket. It generates an overflow bucket name, creates the corresponding page file, initializes the overflow bucket vector, creates and adds an overflow bucket header, modifies the parent bucket's header, and writes both vectors to their respective files.

### getBucketNameFromHeader

```java
static String getBucketNameFromHeader(Vector bucket)
```

This method retrieves the bucket name from the header of a given bucket vector.

### modifyParentBucketHeaderAfterAddingOverflow

```java
static void modifyParentBucketHeaderAfterAddingOverflow(String overflowBucketName, Vector parentBucket)
```

This method modifies the header of a parent bucket vector after adding an overflow bucket. It updates the overflow bucket name in the parent bucket's header.

### createHeaderForOverflowBucket

```java
static void createHeaderForOverflowBucket(Vector overflowBucket, String overflowBucketName, String parentBucketName)
```

This method creates a header for an overflow bucket, associating the overflow bucket name and the parent bucket name with the overflow bucket vector.

### getOverflowBucketName

```java
static String getOverflowBucketName(Vector parentBucket)
```

This method generates an overflow bucket name based on the parent bucket name and the overflow bucket count.

### createPage

```java
public static Vector createPage(Table table) throws IOException
```

This method creates a new page for a given table. It generates a page name, creates the corresponding page file, initializes the

[Continued]

page vector, creates and adds a page header, and writes the page vector to the file. It returns the created page vector.

### createOverflowPage

```java
public static Vector createOverflowPage(Table table, Vector<Hashtable<String, Object>> parentPage) throws IOException
```

This method creates an overflow page for a given table and parent page. It generates an overflow page name, creates the corresponding page file, initializes the overflow page vector, creates and adds an overflow page header, updates the parent page's header, and writes both vectors to their respective files. It returns the created overflow page vector.

### createHeaderForNewPage

```java
public static void createHeaderForNewPage(Vector<Hashtable<String, Object>> pageVector, String pageName)
```

This method creates a header for a new page, setting initial values for various page attributes, such as overflow page name, parent page overflow status, and page name. The header is added to the page vector.

### getPageToInsertIn

```java
public static ArrayList<Vector> getPageToInsertIn(Hashtable<String, Object> colNameValue, ArrayList<String> pages, Table table) throws IOException, ClassNotFoundException
```

This method determines the appropriate page(s) to insert a record based on the given column name-value pair. It iterates over the list of pages, checks the key against the minimum key in each page, and returns the page vector(s) where the record should be inserted. It also includes the next page vector if it exists.

## Usage

To use this class, create an instance of it or call its static methods directly. Make sure to provide the required parameters where necessary. For example:

```java
Table myTable = new Table("employees");
boolean isPageValid = MyClass.checkPageTable(myTable, "employees_page1");
String[] pageList = MyClass.listPages();
ArrayList<String[]> csvData = MyClass.readCSV("data.csv");
Table retrievedTable = MyClass.getTable("employees");
Vector bucket = MyClass.createBucket(myGridIndex);
// etc.
```

Remember to handle exceptions that may be thrown by the methods, such as `IOException` or `ClassNotFoundException`.

## Dependencies

This class depends on other classes and files, such as `Table`, `GridIndex`, and external file paths. Make sure to include and configure these dependencies properly for the class to work correctly.

## License

This class is released under the [MIT License](https://opensource.org/licenses/MIT). Please refer to the LICENSE file for more information.

## Credits

This class was developed by [Your Name]. If you have any questions or suggestions, please contact [Your Email].
