package cmbc.bigdata.utils;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import static org.apache.commons.io.FileUtils.readFileToString;

/**
 * Created by huangpengcheng on 2016/7/20 0020.
 */
public class WholeFileHandler {
    private String filePath;
    private File wholeFile;

    public WholeFileHandler(String filePath){
        this.filePath = filePath;
        this.wholeFile = new File(filePath);
    }

    public WholeFileHandler(File hostsFile){
        this.wholeFile = hostsFile;
        try {
            this.filePath = hostsFile.getCanonicalPath();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public HashMap<String, String> parseHosts() throws IOException {
        HashMap<String, String> hostsMap = new HashMap<String, String>();
        LineIterator li = FileUtils.lineIterator(wholeFile);
        for (; li.hasNext(); ) {
            String str = li.next();
            String[] kv = str.split("\\s+");
            if (kv.length == 2)
                hostsMap.put(kv[0], kv[1]);
        }

        return hostsMap;
    }

    public byte[] fileToBytes() throws IOException {
        return readFileToString(wholeFile,"UTF-8").getBytes();
    }

    /**
     * Parse Hosts File (push mode by kv)
     * @deprecated
     * @return
     * @throws IOException
     */
}
