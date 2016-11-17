package com.threeandahalfroses.aws.ddb.loader;

import org.json.simple.parser.ParseException;

import java.io.IOException;

/**
 * @author patrizz on 12/11/2016.
 * @copyright 2015 Three and a half Roses
 */
public interface DataLoader {
    LoadCsvResult load() throws IOException, ParseException;
}
