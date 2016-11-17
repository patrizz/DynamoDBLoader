package com.threeandahalfroses.aws.ddb.loader;

import com.amazonaws.regions.Region;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.util.*;

/**
 * @author patrizz on 08/11/2016.
 * @copyright 2015 Three and a half Roses
 */
public abstract class BaseDataLoader implements DataLoader {

    private static final Logger LOGGER = LogManager.getLogger(BaseDataLoader.class);

    private final CsvReaderSource csvReaderSource;
    private final DynamoDB dynamodb;
    private final LoaderStateSource loaderStateSource;

    /*
    public BaseDataLoader() {
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider().getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e);
        }
        AmazonDynamoDBClient amazonDynamoDBClient = new AmazonDynamoDBClient(credentials);
        Region euCentral1 = Region.getRegion(Regions.EU_WEST_1);
        amazonDynamoDBClient.setRegion(euCentral1);

        dynamodb = new DynamoDB(amazonDynamoDBClient);
    }
    */

    LoaderState loadLatest() {
        return loaderStateSource.loadLatest();
    }
    void save(LoaderState loaderState, boolean forceToPermanentStorage) {
        loaderStateSource.save(loaderState, forceToPermanentStorage);
    }


    public BaseDataLoader(DynamoDB dynamodb, CsvReaderSource csvReaderSource, LoaderStateSource loaderStateSource) {
        this.dynamodb = dynamodb;
        this.csvReaderSource = csvReaderSource;
        this.loaderStateSource = loaderStateSource;
    }

    @Override
    public LoadCsvResult load() throws IOException, ParseException {
        //check if we have a previous state somewhere
        PreviousState previousState = loadPreviousState();
        if (previousState == null) {
            previousState = new PreviousState();
            previousState.setCreateDate(new Date());
        }
        previousState.setLastUpdate(new Date());
        previousState.setLastRowProcessed(0l);
        savePreviousState(previousState);
        return null;
    }

    protected abstract PreviousState loadPreviousState() throws IOException, ParseException;
    protected abstract void savePreviousState(PreviousState previousState) throws IOException;

    public int saveFileToTable() throws IOException {
        return saveFileToTable(0, -1);
    }
    public int saveFileToTable(int begin, int end) throws IOException {

        Reader reader = getCsvReader();
        CSVParser csvParser = new CSVParser(reader, CSVFormat.EXCEL);
        int count=0;
        Iterator<CSVRecord> recordIterator = csvParser.iterator();
        CsvToItemMapper csvToItemMapper = getCsvToIndexMapper();
        Table table = dynamodb.getTable(getTableName());
        for (int i=begin; recordIterator.hasNext() && (end == -1 || i<end); i++) {
            count++;
            CSVRecord csvRecord = recordIterator.next();
            Item item = csvToItemMapper.mapToItem(csvRecord);
            table.putItem(item);
        }
        return count;
    }

    public interface ItemToStringID {
        String toStringID(Item item);
    }
    public interface RowToStringID {
        String toStringID(CSVRecord record);
    }

    public Reader getCsvReader() throws IOException {
        Reader reader = this.csvReaderSource.getReader();
        BufferedReader bufferedReader = new BufferedReader(reader);
        if (this.csvReaderSource.firstLineContainsHeaders()) {
            bufferedReader.readLine(); //read off the headers...
        }
        return bufferedReader;
    }

    public abstract String getTableName();

    public abstract boolean duplicatesPossible();

    protected abstract ItemToStringID getItemToStringID();
    protected abstract RowToStringID getRowToStringID();



    public Map<String, String> loadAllItemIDsFromFile() throws IOException {

        Reader reader = getCsvReader();
        CSVParser csvParser = new CSVParser(reader, CSVFormat.EXCEL);
        Iterator<CSVRecord> iterator = csvParser.iterator();
        Map<String, String> stringList = new HashMap<String, String>();
        int i = 0;
        long start = System.currentTimeMillis();
        long end = start;
        LOGGER.info("processing file for table: " + getTableName());
        while (iterator.hasNext()) {
            CSVRecord record = iterator.next();
            String id = getRowToStringID().toStringID(record);
            stringList.put(id, id);
            if (i%10000==0) {
                end = System.currentTimeMillis();
                LOGGER.info("Processed " + i + " row items - time taken " + (end-start) + " record items.  Freemem: " + getFreemem());
                start = System.currentTimeMillis();
            }
            i++;
        }
        LOGGER.info("processed total of " + i + " rows from csv for table: " + getTableName());
        return stringList;
    }

    public Map<String, String> loadAllItemIDsFromTable() {
        Table table = this.dynamodb.getTable(getTableName());
        ItemCollection<ScanOutcome> itemCollection = table.scan(new ScanFilter("uid").contains(":"));
        Iterator<Item> iterator = itemCollection.iterator();
        Map<String, String> stringList = new HashMap<String, String>();
        int i = 0;
        long start = System.currentTimeMillis();
        long end = start;

        LOGGER.info("processing table: " + getTableName());
        while (iterator.hasNext()) {
            Item item = iterator.next();
            String id = getItemToStringID().toStringID(item);
            stringList.put(id, id);
            if (i%10000==0) {
                end = System.currentTimeMillis();
                LOGGER.info("Processed " + i + " DynamodDB items - time taken " + (end-start) + " DynamodDB items.  Freemem: " + getFreemem());
                start = System.currentTimeMillis();
            }
            i++;
        }
        LOGGER.info("processed total of " + i + " rows from table: " + getTableName());
        return stringList;
    }

    private long getFreemem() {
        return (long)(Runtime.getRuntime().freeMemory()/1048576);
    }

    public Diffs getDiffs() throws IOException {

        LOGGER.info("Getting diff for table "+getTableName());

        Map<String, String> fileIds = loadAllItemIDsFromFile();
        LOGGER.info("file loaded for table "+getTableName());

        Map<String, String> tableIds = loadAllItemIDsFromTable();
        LOGGER.info("tableids loaded for table "+getTableName());

        List<String> deleteTableIds = new ArrayList<>();

        LOGGER.info("Getting diff for table "+getTableName());
        long start = System.currentTimeMillis();
        long end = start;

        int d=0;
        int i=0;
        Collection<String> tableIdValues = tableIds.values();
        for (Iterator<String> tableIdValuesIterator=tableIdValues.iterator(); tableIdValuesIterator.hasNext(); ) {
            String tid=tableIdValuesIterator.next();
            if (!fileIds.containsKey(tid)) {
                deleteTableIds.add(tid);
                d++;
            }
            if (i%10000==0) {
                end = System.currentTimeMillis();
                LOGGER.info("Checked diffs on " + i + " items - time taken " + (end-start) + " DynamodDB items.  Freemem: " + getFreemem());
                LOGGER.info("Added " + d + " delete table ids");
                start = System.currentTimeMillis();

            }
            i++;
        }

        LOGGER.info("Done diff for table "+getTableName());
        Diffs diffs = new Diffs(deleteTableIds);
        return diffs;
    }

    public void deleteAllItems() {
        System.err.println("start deleteAllItems from table: " + getTableName());
        Table table = this.dynamodb.getTable(getTableName());
        ItemCollection<ScanOutcome> itemCollection = table.scan(new ScanFilter("uid").contains(":"));
        Iterator<Item> iterator = itemCollection.iterator();
        long deleteCounter = 0;
        while (iterator.hasNext()) {
            deleteCounter++;
            Item item = iterator.next();
            DeleteItemSpec deleteItemSpec = new DeleteItemSpec();
            deleteItemSpec.withPrimaryKey("uid", item.getString("uid"));
            table.deleteItem(deleteItemSpec);
            if (deleteCounter%25==0) {
                System.err.println("deleted: " + deleteCounter);
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        System.err.println("done deleteAllItems");
    }

    public abstract CsvToItemMapper getCsvToIndexMapper();

    public String loadDataFromFolder(File folder) throws IOException {

        if (folder.isDirectory()) {
            File[] files = folder.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".csv");
                }
            });
            for (File file:files) {
                LOGGER.debug("file: " + file.getAbsoluteFile());
                loadDataFromFile(file);
            }
            return "ok";
        } else {
            return "nok";
        }
    }

    public Long loadDataFromFile(File file) throws IOException {
        return LoaderUtility.loadDataFromFile(
                this.dynamodb,
                this.dynamodb.getTable(getTableName()),
                file,
                getCsvToIndexMapper(),
                getDelimiter()
                );

    }

    protected char getDelimiter() {
        return ',';
    }

    protected abstract File getOutputFolder() throws IOException;


}

