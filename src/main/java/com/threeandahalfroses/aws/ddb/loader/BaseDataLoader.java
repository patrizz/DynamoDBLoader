package com.threeandahalfroses.aws.ddb.loader;

import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.BatchWriteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.nio.Buffer;
import java.util.*;

/**
 * @author patrizz on 08/11/2016.
 * @copyright 2015 Three and a half Roses
 */
public abstract class BaseDataLoader implements DataLoader {

    private static final Logger LOGGER = LogManager.getLogger(BaseDataLoader.class);

    private final CsvReaderSource csvReaderSource;
    private final DynamoDB dynamodb;
    private final LoaderStateStore loaderStateStore;

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
        return loaderStateStore.loadLatest();
    }

    void save(LoaderState loaderState) throws IOException {
        loaderStateStore.save(loaderState);
    }
    void save(LoaderState loaderState, boolean forceToPermanentStorage) throws IOException {
        loaderStateStore.save(loaderState, forceToPermanentStorage);
    }

    public BaseDataLoader(DynamoDB dynamodb, CsvReaderSource csvReaderSource, LoaderStateStore loaderStateStore) {
        this.dynamodb = dynamodb;
        this.csvReaderSource = csvReaderSource;
        this.loaderStateStore = loaderStateStore;
    }

    @Override
    public LoadCsvResult load() throws IOException, ParseException {
        //check if we have a previous state somewhere

        LOGGER.info("Getting latest loader state for table: " + getTableName());
        LoaderState loaderState = loadLatest();
        if (loaderState != null) {
            LOGGER.info("Latest state found: " + loaderState.toJSONString());
        } else {
            LOGGER.info("No latest state found, creating one");
            loaderState = new LoaderState(LoaderState.StateName.STARTING);
            loaderStateStore.save(loaderState, true);
        }

        if (loaderState.getStateName() == LoaderState.StateName.STARTING) {
            LOGGER.info("STARTING");
            File file = File.createTempFile("tmp", ".csv");
            Diffs diffs = getDiffs();
            Iterator<String> deletedIdsIterator = diffs.getDeletedFileIds().iterator();
            FileWriter fileWriter = new FileWriter(file);
            LOGGER.debug("writing ids to csv file");
            while (deletedIdsIterator.hasNext()) {
                String id = deletedIdsIterator.next();
                fileWriter.write(id);
                fileWriter.write("\n");
            }
            LOGGER.debug("done writing ids to csv file");
            fileWriter.close();
            LOGGER.info("done STARTING");
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("filename", file.getAbsoluteFile());
            loaderState.changeState(
                    LoaderState.StateName.DIFFS_DETERMINED_AND_SAVED,
                    jsonObject
            );
            loaderStateStore.save(loaderState, true);
        }

        if (loaderState.getStateName() == LoaderState.StateName.DIFFS_DETERMINED_AND_SAVED) {
            LOGGER.info("Deleting items from dynamodb");
            FileReader fileReader = new FileReader(new File((String)loaderState.getStateVariables().get("filename")));
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String readLine = null;
            BatchWriteItemSpec batchWriteItemSpec = new BatchWriteItemSpec();
            TableWriteItems tableWriteItems = new TableWriteItems(getTableName());
            while ((readLine = bufferedReader.readLine()) != null) {
                tableWriteItems.addPrimaryKeyToDelete(new PrimaryKey("uid", readLine));
            }
            batchWriteItemSpec.withTableWriteItems(tableWriteItems);




            LOGGER.info("items deleted from dynamodb");
            loaderState.changeState(LoaderState.StateName.DELETED_ITEMS_REMOVED_FROM_DYNAMODB);
            loaderStateStore.save(loaderState, true);
        }

        if (loaderState.getStateName() == LoaderState.StateName.DELETED_ITEMS_REMOVED_FROM_DYNAMODB) {
            LOGGER.info("file processing starting");
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("at-row", 0);
            loaderState.changeState(LoaderState.StateName.PROCESSING_FILE, jsonObject);
            loaderStateStore.save(loaderState, true);
            LOGGER.info("file processing started");
        }

        if (loaderState.getStateName() == LoaderState.StateName.PROCESSING_FILE) {
            LOGGER.info(String.format("processing file at row %d", loaderState.getStateVariables().get("at-row")));

            JSONObject jsonObject = loaderState.getStateVariables();
            int atRow = (int) jsonObject.getOrDefault("at-row", 0);

            BufferedReader csvReader = new BufferedReader(getCsvReader());
            //skip already processed rows as much as possible
            String readLine = null;
            long skipped = 0;
            for (; skipped<atRow; skipped++) {
                readLine = csvReader.readLine();
                if (readLine==null) {
                    JSONObject jsonObject1 = new JSONObject();
                    jsonObject1.put("at-row", 0);
                    jsonObject1.put("processed", skipped);
                    loaderState.changeState(LoaderState.StateName.FILE_PROCESSED, jsonObject1);
                    loaderStateStore.save(loaderState, true);
                    break;
                }
                jsonObject.put("at-row", skipped);
                loaderState.changeState(LoaderState.StateName.PROCESSING_FILE, jsonObject);
                loaderStateStore.save(loaderState);
            }
            //now load the others
            CSVParser csvParser = new CSVParser(csvReader, CSVFormat.EXCEL);
            Iterator<CSVRecord> iterator = csvParser.iterator();
            CsvToItemMapper mapper = getCsvToIndexMapper();

            TableWriteItems tableWriteItems = new TableWriteItems(getTableName());
            long counter = 0;
            JSONObject jsonObject1 = new JSONObject();

            while (iterator.hasNext()) {
                CSVRecord csvRecord = iterator.next();
                Item item = mapper.mapToItem(csvRecord);
                tableWriteItems.addItemToPut(item);
                counter++;
                if (counter%20==0) {
                    batchWrite(skipped, tableWriteItems, counter);
                    tableWriteItems = new TableWriteItems(getTableName());
                }
                jsonObject1.put("at-row", 0);
                jsonObject1.put("processed", (skipped+counter));
                loaderState.changeState(LoaderState.StateName.PROCESSING_FILE, jsonObject1);
                loaderStateStore.save(loaderState);
            }
            if (tableWriteItems != null && tableWriteItems.getItemsToPut() != null && tableWriteItems.getItemsToPut().size() > 0) {
                batchWrite(skipped, tableWriteItems, counter);
            }

            jsonObject1.put("at-row", 0);
            jsonObject1.put("processed", (skipped + counter));
            loaderState.changeState(LoaderState.StateName.FILE_PROCESSED, jsonObject1);
            loaderStateStore.save(loaderState, true);
            LOGGER.info("processed all items to dynamodb, totalling: " + (skipped + counter));
        }

        return null;
    }

    private TableWriteItems batchWrite(long skipped, TableWriteItems tableWriteItems, long counter) {
        LOGGER.info("writing items to dynamodb");
        BatchWriteItemSpec batchWriteItemSpec = new BatchWriteItemSpec();
        batchWriteItemSpec.withTableWriteItems(tableWriteItems);
        BatchWriteItemOutcome outcome = dynamodb.batchWriteItem(batchWriteItemSpec);
        do {
            // Check for unprocessed keys which could happen if you exceed provisioned throughput
            Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();
            if (outcome.getUnprocessedItems().size() == 0) {
                LOGGER.debug("No unprocessed items found");
            } else {
                LOGGER.debug("Retrieving the unprocessed items");
                outcome = dynamodb.batchWriteItemUnprocessed(unprocessedItems);
            }
        } while (outcome.getUnprocessedItems().size() > 0);
        tableWriteItems = new TableWriteItems(getTableName());
        LOGGER.info("written items to dynamodb, totalling: " + counter + " and first skipped " + skipped + " items.");
        return tableWriteItems;
    }

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
        ItemCollection<ScanOutcome> itemCollection = table.scan(new ScanFilter("uid").exists());
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

