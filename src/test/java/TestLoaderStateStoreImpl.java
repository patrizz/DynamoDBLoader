import com.threeandahalfroses.aws.ddb.loader.LoaderState;
import com.threeandahalfroses.aws.ddb.loader.LoaderStateStore;
import com.threeandahalfroses.commons.general.JSONUtility;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * @author Patrice Kerremans
 * @copyright 2014 Three and a half Roses
 */
public class TestLoaderStateStoreImpl implements LoaderStateStore {

    public static final String ATTR_NAME = "name";
    public static final String ATTR_VARIABLES = "variables";
    private final File tempStateFile;

    public TestLoaderStateStoreImpl(File tempStateFile) {
        this.tempStateFile = tempStateFile;
    }

    public TestLoaderStateStoreImpl(String tempStateFilename) {
        this.tempStateFile = new File(tempStateFilename);
    }

    @Override
    public LoaderState loadLatest() {
        LoaderState loaderState = null;
        try {
            JSONObject jsonObject = (JSONObject) JSONUtility.toJSONAware(new FileReader(this.tempStateFile));
            loaderState = new LoaderState(
                    LoaderState.StateName.valueOf((String) jsonObject.get(ATTR_NAME)),
                    (JSONObject) jsonObject.get(ATTR_VARIABLES)
            );
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return loaderState;
    }

    private String getJSONString(LoaderState loaderState) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(ATTR_NAME, loaderState.getStateName().name());
        jsonObject.put(ATTR_VARIABLES, loaderState.getStateVariables());
        return jsonObject.toJSONString();
    }

    @Override
    public void save(LoaderState loaderState) throws IOException {
        save(loaderState, false);
    }


    @Override
    public void save(LoaderState loaderState, boolean forceToPermanentStorage) throws IOException {
        String jsonString = getJSONString(loaderState);
        IOUtils.write(jsonString, new FileWriter(tempStateFile));
    }

    @Override
    public void forceToPermanentStorage() throws IOException {
        //it's a test so don't do anything now
    }
}
