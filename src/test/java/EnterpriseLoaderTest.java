import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.model.*;
import com.threeandahalfroses.aws.ddb.loader.Diffs;
import com.threeandahalfroses.aws.ddb.loader.EnterpriseLoader;
import com.threeandahalfroses.aws.ddb.loader.LoadCsvResult;
import com.threeandahalfroses.aws.ddb.loader.LoaderUtility;
import com.threeandahalfroses.commons.aws.dynamodb.DynamoDBUtility;
import com.threeandahalfroses.commons.general.JSONUtility;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONAware;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author patrizz on 10/11/2016.
 * @copyright 2015 Three and a half Roses
 */

public class EnterpriseLoaderTest {

    Logger LOGGER = LogManager.getLogger(EnterpriseLoaderTest.class);

    DynamoDB dynamodb = null;

    String filename = null;
    String filename2 = null;

    @Before
    public void setup() {
        System.setProperty("sqlite4java.library.path", "native-libs");
        URL csvFile = EnterpriseLoaderTest.class.getResource("enterprise1.csv");
        filename = csvFile.getFile();
        URL csvFile2 = EnterpriseLoaderTest.class.getResource("enterprise2.csv");
        filename2 = csvFile2.getFile();
        dynamodb = new DynamoDB(DynamoDBEmbedded.create().amazonDynamoDB());
    }


    private Table createTable(String tableName) {
        List<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement("uid", KeyType.HASH));


        List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
        attributeDefinitions.add(new AttributeDefinition("uid", ScalarAttributeType.S));

        ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput();
        provisionedThroughput.setReadCapacityUnits(1000l);
        provisionedThroughput.setWriteCapacityUnits(1000l);
        Table table = dynamodb.createTable(tableName, keySchema, attributeDefinitions, provisionedThroughput);
        if (table == null) {
            LOGGER.warn("table returned when creating it is null");
        }
        CreateGlobalSecondaryIndexAction createGlobalSecondaryIndexAction =
                new CreateGlobalSecondaryIndexAction()
                        .withIndexName("enterpriseNumber-index")
                        .withKeySchema(keySchema)
                        .withProvisionedThroughput(new ProvisionedThroughput().withWriteCapacityUnits(1000l).withReadCapacityUnits(1000l))
                        .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY));
        table.createGSI(
                createGlobalSecondaryIndexAction,
                new AttributeDefinition("enterpriseNumber", ScalarAttributeType.S)
        );
        return table;
    }

    @Test
    public void test_diffs_empty_table() throws IOException {
        File tempFile = File.createTempFile("tmp", "el");
        EnterpriseLoader enterpriseLoader = new EnterpriseLoader(
                dynamodb,
                new FileBasedCsvReaderSource(filename, true),
                new TestLoaderStateStoreImpl(tempFile)
        );

        createTable(enterpriseLoader.getTableName());

        Diffs diffs = enterpriseLoader.getDiffs();
        assertNotNull("diffs null", diffs);
        assertNotNull("diffs delete ids null", diffs.getDeletedFileIds());
        assertEquals("0 to delete because table is empty :)", 0, diffs.getDeletedFileIds().size());
    }

    @Test
    public void test_diffs_filled_table() throws IOException {
        /*
            Deleted 11 rows in enterprise2.csv (wrt enterprise1.csv)
            "0200.362.408","AC","000","2","008",01-01-1968
            "0202.500.267","AC","012","2","116",01-01-1968
            "0202.492.151","AC","012","2","116",29-12-1955
            "0202.500.465","AC","000","2","116",01-01-1968
            "0202.508.878","AC","000","2","008",01-01-1968
            "0202.554.113","AC","000","2","108",16-05-1963
            "0203.978.726","AC","000","2","008",30-12-1929
            "0203.980.409","AC","000","2","108",01-01-1968
            "0205.157.869","AC","000","2","416",30-05-1964
            "0205.264.668","AC","000","2","417",24-12-1924
            "0205.350.681","AC","000","2","416",01-03-1966
         */

        EnterpriseLoader enterpriseLoader = new EnterpriseLoader(dynamodb, new FileBasedCsvReaderSource(filename2, true), new TestLoaderStateStoreImpl(File.createTempFile("test", "el")));

        Table table = createTable(enterpriseLoader.getTableName());

        //first: load the table
        BufferedReader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/enterprise1.csv")));
        reader.readLine(); //read off the headers

        long count = LoaderUtility.loadReaderIntoTable(
                dynamodb,
                table,
                reader,
                enterpriseLoader.getCsvToIndexMapper(),
                ','
        );
        assertEquals("wrong number loaded from filename1", 429l, count);

        //now check diffs (should check deletions wrt filename2)
        Diffs diffs = enterpriseLoader.getDiffs();
        assertNotNull("diffs is null", diffs);
        assertNotNull("diffs deletedFileIds is null", diffs.getDeletedFileIds());
        assertEquals("diffs count is wrong", 11, diffs.getDeletedFileIds().size());

    }

    @Test
    public void test_loadFileIDs() throws IOException {
        EnterpriseLoader enterpriseLoader = new EnterpriseLoader(dynamodb, new FileBasedCsvReaderSource(filename, true), new TestLoaderStateStoreImpl(File.createTempFile("test", "el")));
        Map<String, String> maps = enterpriseLoader.loadAllItemIDsFromFile();
        assertEquals("wrong number of items", 429, maps.values().size());
    }

    @Test
    public void test_saveToTable_partially() throws IOException {
        EnterpriseLoader enterpriseLoader = new EnterpriseLoader(dynamodb, new FileBasedCsvReaderSource(filename, true), new TestLoaderStateStoreImpl(File.createTempFile("test", "el")));
        String tableName = enterpriseLoader.getTableName();
        createTable(tableName);
        enterpriseLoader.saveFileToTable(0, 10);
        Long itemCount = DynamoDBUtility.countItems(dynamodb, tableName);
        assertEquals("wrong item count", new Long(10), itemCount);
        enterpriseLoader.saveFileToTable(0, 142);
        Long itemCount2 = DynamoDBUtility.countItems(dynamodb, tableName);
        //note that we'd expect to have 142 items and not 152 or 132 items in the DB
        // we'd expect the 10 existing to be overwritten and hence in total have 142 in total
        assertEquals("wrong item count2", new Long(142), itemCount2);
    }

    @Test
    public void test_load() throws IOException, ParseException {
        File tempFile1 = File.createTempFile("test", ".el");
        LOGGER.info("temp file: " + tempFile1.getAbsolutePath());
        EnterpriseLoader enterpriseLoader1 = new EnterpriseLoader(dynamodb, new FileBasedCsvReaderSource(filename, true), new TestLoaderStateStoreImpl(tempFile1));
        String tableName = enterpriseLoader1.getTableName();
        createTable(tableName);
        enterpriseLoader1.saveFileToTable(0, 142);

        File tempFile2 = File.createTempFile("test", ".el");
        EnterpriseLoader enterpriseLoader2 = new EnterpriseLoader(dynamodb, new FileBasedCsvReaderSource(filename2, true), new TestLoaderStateStoreImpl(tempFile2));
        LoadCsvResult result = enterpriseLoader2.load();

        JSONAware jsonAware = JSONUtility.toJSONAware(new FileReader(tempFile2));
        assertNotNull("JSONAware null", jsonAware);
        assertEquals("JSONAware not object", JSONObject.class, jsonAware.getClass());
        JSONObject jsonObject = (JSONObject) jsonAware;
        assertNotNull("name null", jsonObject.get("name"));
        assertNotNull("variables null", jsonObject.get("variables"));
        JSONObject variablesJsonObject = (JSONObject) jsonObject.get("variables");
        assertEquals("processed rows wrong", 520l, variablesJsonObject.get("processed"));
    }



}
