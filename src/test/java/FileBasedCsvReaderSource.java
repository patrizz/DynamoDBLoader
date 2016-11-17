import com.threeandahalfroses.aws.ddb.loader.CsvReaderSource;
import com.threeandahalfroses.commons.aws.s3.FileBasedS3DataSource;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;

/**
 * @author patrizz on 11/11/2016.
 * @copyright 2015 Three and a half Roses
 */
public class FileBasedCsvReaderSource implements CsvReaderSource {

    private final String filename;
    private final Boolean firstLineContainsHeaders;

    public FileBasedCsvReaderSource(String filename, Boolean firstLineContainsHeaders) {
        this.filename = filename;
        this.firstLineContainsHeaders = firstLineContainsHeaders;
    }

    @Override
    public boolean firstLineContainsHeaders() {
        return firstLineContainsHeaders;
    }

    @Override
    public Reader getReader() throws FileNotFoundException {
        return new FileReader(new File(this.filename));
    }
}
