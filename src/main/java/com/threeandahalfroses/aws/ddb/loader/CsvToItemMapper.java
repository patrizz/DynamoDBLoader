package com.threeandahalfroses.aws.ddb.loader;

import com.amazonaws.services.dynamodbv2.document.Item;
import org.apache.commons.csv.CSVRecord;

/**
 * @author patrizz on 12/11/2016.
 * @copyright 2015 Three and a half Roses
 */
public interface CsvToItemMapper {
    Item mapToItem(CSVRecord csvRecord);
}
