package com.threeandahalfroses.aws.ddb.loader;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.common.io.Files;
import com.threeandahalfroses.commons.aws.s3.S3Utility;
import com.threeandahalfroses.commons.general.JSONUtility;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.io.*;

/**
 * @author patrizz on 10/11/2016.
 * @copyright 2015 Three and a half Roses
 */
public class EnterpriseLoader extends BaseDataLoader {

    private static final Logger LOGGER = LogManager.getLogger(EnterpriseLoader.class);


    private CsvToItemMapper enterpriseMapper = new EnterpriseMapper();
    private ItemToStringID itemToStringID = new ItemToStringID() {

        @Override
        public String toStringID(Item item) {
            return item.getString("enterpriseNumber");
        }
    };
    private RowToStringID rowToStringID = new RowToStringID() {
        @Override
        public String toStringID(CSVRecord record) {
            return record.get(0);
        }
    };

    public static class EnterpriseMapper implements CsvToItemMapper {

        @Override
        public Item mapToItem(CSVRecord csvRecord) {
            try {
                String enterpriseNumber = csvRecord.get(0);
                String status = csvRecord.get(1);
                String juridicalSituation = csvRecord.get(2);
                String typeOfEnterprise = csvRecord.get(3);
                String juridicalForm = csvRecord.get(4);
                String startDate = csvRecord.get(5);

                if (StringUtils.isEmpty(juridicalForm)) {
                    juridicalForm = "UUU"; //unknown
                }

                if (
                        StringUtils.isEmpty(enterpriseNumber) ||
                                StringUtils.isEmpty(status) ||
                                StringUtils.isEmpty(juridicalSituation) ||
                                StringUtils.isEmpty(typeOfEnterprise) ||
                                StringUtils.isEmpty(juridicalForm) ||
                                StringUtils.isEmpty(startDate)
                        ) {
                    LOGGER.warn("*** something mandatory is null: " + csvRecord.toString());
                    return null;
                }

                Item item = new Item();
                String uid = enterpriseNumber
                        + ":" + status
                        + ":" + juridicalSituation
                        + ":" + typeOfEnterprise
                        + ":" + juridicalForm;
                LOGGER.trace(uid);
                item
                        .withPrimaryKey("uid", uid)
                        .with("enterpriseNumber", enterpriseNumber)
                        .with("status", status)
                        .with("juridicalSituation", juridicalSituation)
                        .with("typeOfEnterprise", typeOfEnterprise)
                        .with("juridicalForm", juridicalForm)
                        .with("startDate", startDate);

                return item;
            } catch(ArrayIndexOutOfBoundsException AIOOBe) {
                throw new RuntimeException("error processing: " + csvRecord.toString(), AIOOBe);
            }
        }
    }

    public EnterpriseLoader(DynamoDB dynamodb, CsvReaderSource csvReaderSource) {
        super(dynamodb, csvReaderSource);
    }

    @Override
    protected PreviousState loadPreviousState() throws IOException, ParseException {
        return new PreviousState((JSONObject) JSONUtility.toJSONAware(S3Utility.getReader(Region.getRegion(Regions.EU_CENTRAL_1), "quitus-base", "enterprise-load-state.json")));
    }

    @Override
    protected void savePreviousState(PreviousState previousState) throws IOException {
        S3Utility.save(previousState.toJSONString(), Region.getRegion(Regions.EU_CENTRAL_1), "quitus-base", "enterprise-load-state.json", null, null);
    }


    @Override
    public String getTableName() {
        return "od_enterprise";
    }

    @Override
    public boolean duplicatesPossible() {
        return false;
    }

    @Override
    protected ItemToStringID getItemToStringID() {
        return this.itemToStringID;
    }

    @Override
    protected RowToStringID getRowToStringID() {
        return this.rowToStringID;
    }

    @Override
    public CsvToItemMapper getCsvToIndexMapper() {
        CsvToItemMapper csvToItemMapper = new CsvToItemMapper() {

            @Override
            public Item mapToItem(CSVRecord csvRecord) {
                return enterpriseMapper.mapToItem(csvRecord);
            }
        };
        return csvToItemMapper;
    }

    @Override
    protected File getOutputFolder() throws IOException {
        return Files.createTempDir();
    }
}
