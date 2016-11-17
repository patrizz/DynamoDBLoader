package com.threeandahalfroses.aws.ddb.loader;

/**
 * @author patrizz on 12/11/2016.
 * @copyright 2015 Three and a half Roses
 */
public class LoadCsvResult {
    long numberOfDeletedItems;
    long numberOfNewItemsLoaded;
    long numberOfItemsUpdated;
    long lastProcessedCsvRecordIndex;
}
