package com.threeandahalfroses.aws.ddb.loader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;

/**
 * @author patrizz on 11/11/2016.
 * @copyright 2015 Three and a half Roses
 */
public interface CsvReaderSource {
    boolean firstLineContainsHeaders();
    Reader getReader() throws IOException;
}
