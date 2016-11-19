package com.threeandahalfroses.aws.ddb.loader;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.BatchWriteItemSpec;
import com.threeandahalfroses.commons.general.Utility;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

/**
 * @author patrizz on 12/11/2016.
 * @copyright 2015 Three and a half Roses
 */
public class LoaderUtility {
    public static long loadReaderIntoTable(DynamoDB dynamoDB, Table table, Reader reader, CsvToItemMapper csvToItemMapper, char delimiter) throws IOException {
        Iterable<CSVRecord> records = CSVFormat.EXCEL.withDelimiter(delimiter).parse(reader);
        //"EntityNumber","ActivityGroup","NaceVersion","NaceCode","Classification"

        long recordCounter = 0;
        long start = System.currentTimeMillis();
        long end = System.currentTimeMillis();
        BatchWriteItemSpec batchWriteItemSpec = new BatchWriteItemSpec();
        TableWriteItems tableWriteItems = new TableWriteItems(table.getTableName());
        for (CSVRecord csvRecord : records) {

            recordCounter++;
            if (recordCounter == 1) {
                //skip header!
                continue;
            }

            Item item = csvToItemMapper.mapToItem(csvRecord);
            if (item == null || item.isNull("code")) {
                Utility.logIt("+++ item is null or the code is null");
                continue;
            }
            tableWriteItems.addItemToPut(item);

            if (recordCounter % 25 == 0 && tableWriteItems.getItemsToPut().size()>0) {
                batchWriteItemSpec.withTableWriteItems(tableWriteItems);
                try {
                    dynamoDB.batchWriteItem(batchWriteItemSpec);
                } catch (Throwable t) {
                    t.printStackTrace();
                    System.err.println("fix this first");
                    System.exit(-1);
                }
                end = System.currentTimeMillis();
                Utility.logIt("> " + recordCounter + " [" + (end - start) + "]");
                start = System.currentTimeMillis();
                batchWriteItemSpec = new BatchWriteItemSpec();
                tableWriteItems = new TableWriteItems(table.getTableName());
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        Utility.logIt("record count: " + recordCounter);



        return recordCounter;

    }

    public static long loadDataFromFile(
            DynamoDB dynamoDB,
            Table table,
            File file,
            CsvToItemMapper csvToItemMapper,
            char delimiter) throws IOException {

        Utility.logIt("loading data from file: " + file.getAbsolutePath());

        Utility.logIt("using mapper: " + (csvToItemMapper == null? "null": csvToItemMapper.getClass().getName()));

        FileReader fileReader = new FileReader(file);
        System.out.println("Successfully loaded " + file.getAbsolutePath() + ".");
        return loadReaderIntoTable(dynamoDB, table, fileReader, csvToItemMapper, delimiter);
    }
}
