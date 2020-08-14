package cmbc.bigdata.core;

import cmbc.bigdata.constants.CHANGEMODE;
import cmbc.bigdata.constants.CONSTANTSUTIL;
import cmbc.bigdata.constants.FILETYPE;
import cmbc.bigdata.constants.PULLMODE;
import cmbc.bigdata.utils.XMLHandler;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.dom4j.DocumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by huangpengcheng on 2016/7/20 0020.
 */
public class Puller {
    private static final Logger logger = LoggerFactory.getLogger(Puller.class);
    private final static ExecutorService EXECUTOR_SERVICE = Executors.newSingleThreadExecutor();
    private final CuratorFramework client;
    private final FILETYPE fileType;
    private final String[] pullFiles;
    private final PULLMODE pmode;
    private final String callBack;
    private XMLHandler xmlHandler;
    private final CHANGEMODE cmode;

    public Puller(CuratorFramework client, FILETYPE fileType, String pullFileName, PULLMODE pmode, String callBack, CHANGEMODE cmode) throws DocumentException {
        this.client = client;
        this.fileType = fileType;
        this.pullFiles= pullFileName.split(",");
        this.pmode = pmode;
        this.callBack = callBack;
        this.cmode = cmode;
        if (fileType==FILETYPE.XML)
            this.xmlHandler = new XMLHandler(pullFileName);
        if (this.pullFiles.length == 0) {
            throw new UnknownFormatFlagsException("Error: Wrong Watch File List format");
        }
    }

    /**
     * Watch One Node Change
     *
     * @throws Exception
     */
    public void watchDataChanged() throws Exception {
        for( final String file : pullFiles){
            String fileName = file.substring(file.lastIndexOf('/') + 1);
            final NodeCache nodeCache = new NodeCache(client,  "/"  + fileName);
            nodeCache.getListenable().addListener(
                    new NodeCacheListener() {
                        public void nodeChanged() throws Exception {
                            ChildData data = nodeCache.getCurrentData();
                            if (null != data) {
                                logger.info("node data changed, new data:\n" + new String(nodeCache.getCurrentData().getData()));
                                bakFile(file);
                                //according to the Change Mode,choose suitable function
                                if (cmode == CHANGEMODE.OVERWRITE) {
                                    overwriteChildDataToLocalFile(file,new String(data.getData()));
                                }else if (cmode == CHANGEMODE.APPEND) {
                                    appendChildDataToLocalFile(new String(data.getData()),file);
                                }
                                logger.info(file + " has been saved from this pulling");
                                if (callBack!=null){
                                    new ProcessExecutor().command(callBack,file).redirectOutput(
                                            Slf4jStream.of(logger).asInfo()
                                    ).execute();
                                }
                            }
                        }
                    }, EXECUTOR_SERVICE);
            nodeCache.start(true);
        }

    }

    /**
     * Watch ths Child node of a path
     * @Todo   Unfinished Method
     * @throws Exception
     */
    public void watchChildrenChanged() throws Exception {
        PathChildrenCache cache = new PathChildrenCache(client,  "/"  + this.pullFiles, true);
        cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        cache.getListenable().addListener(new PathChildrenCacheListener() {

            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                String configPath = event.getData().getPath();
                String configName = configPath.substring(configPath.lastIndexOf( "/" ) + 1);
                String configContent = new String(event.getData().getData());
                if (fileType == FILETYPE.XML) {
                    switch (event.getType()) {
                        case CHILD_ADDED:
                        case CHILD_UPDATED:
                            logger.info("Add/Update the config:" + configPath);
                            xmlHandler.updateOrCreatePropInXML(configName, configContent);
                            xmlHandler.writeToXML();
                            printEventInfo(event);
                            break;
                        case CHILD_REMOVED:
                            logger.info("Remove the config:" + configPath);
                            xmlHandler.deletePropInXML(configName);
                            xmlHandler.writeToXML();
                            printEventInfo(event);
                            break;
                        case CONNECTION_SUSPENDED:
                            break;
                        case CONNECTION_RECONNECTED:
                            break;
                        case CONNECTION_LOST:
                            break;
                        case INITIALIZED:
                            break;
                        default:
                            printEventInfo(event);
                            break;
                    }
                }

                if (fileType == FILETYPE.PLAIN) {
                    switch (event.getType()) {
                        case CHILD_ADDED:
                        case CHILD_UPDATED:
                            logger.info("Add/Update the config:" + configPath);
                            printEventInfo(event);
                            break;
                        case CHILD_REMOVED:
                            logger.info("Remove the config:" + configPath);
                            printEventInfo(event);
                            break;
                        case CONNECTION_SUSPENDED:
                            break;
                        case CONNECTION_RECONNECTED:
                            break;
                        case CONNECTION_LOST:
                            break;
                        case INITIALIZED:
                            break;
                        default:
                            printEventInfo(event);
                            break;
                    }
                }
            }

            private void printEventInfo(PathChildrenCacheEvent event) {
                logger.info("======================================");
                logger.info("Event Type:" + event.getType().toString());
                logger.info("Event Path:" + event.getData().getPath());
                logger.info("Event Content:" + new String(event.getData().getData()));
                logger.info("Event Data Version:" + String.valueOf(event.getData().getStat().getVersion()) + "\n");
            }

        });

    }

    public void pullFromZK() throws Exception {
        if (fileType == FILETYPE.XML) {
            if(pmode == PULLMODE.ONCE) {
                pullXMLOnce();
            }
            else if(pmode == PULLMODE.WATCH){
                watchChildrenChanged();
            }
            else{
                throw new UnsupportedOperationException();
            }
        }

        if (fileType == FILETYPE.PLAIN) {
            if(pmode == PULLMODE.ONCE){
                pullPlainOnce();
            }
            else if(pmode == PULLMODE.WATCH){
                CountDownLatch latch = new CountDownLatch(1);
                watchDataChanged();
                latch.await();
            }
            else{
                throw new UnsupportedOperationException();
            }
        }
    }

    private void pullXMLOnce() throws Exception {
        for(String file : pullFiles) {
            String fileName = file.substring(file.lastIndexOf('/') + 1);
            List<String> childNodes = client.getChildren().forPath("/" + fileName);
            StringBuilder keyBuilder = new StringBuilder();
            String value;
            if (!(new File(file)).exists()) {
                //File nonexist, only creat e element
                xmlHandler.getDocument().addElement("configuration");
                for (String child : childNodes) {
                    logger.info("Pull config: " + child);
                    keyBuilder.append("/").append(fileName).append("/").append(child);
                    value = new String(client.getData().forPath(keyBuilder.toString()));
                    xmlHandler.createPropInXML(child, value);
                    logger.info("Create Config name: " + child +
                            " ,Config value:" + value.replace(CONSTANTSUTIL.VALUE_DESC_SPLIT, ", Config description:"));
                    keyBuilder.setLength(0);
                }
            } else {  //File exist, update or create element
                for (String child : childNodes) {
                    logger.info("Pull config: " + child);
                    keyBuilder.append("/").append(fileName).append("/").append(child);
                    value = new String(client.getData().forPath(keyBuilder.toString()));
                    xmlHandler.updateOrCreatePropInXML(child, value);
                    keyBuilder.setLength(0);
                    logger.info("Update Config name: " + child +
                            " ,Config value:" + value.replace(CONSTANTSUTIL.VALUE_DESC_SPLIT, ", Config description:"));
                }
            }
            xmlHandler.writeToXML();
        }
    }

    private void pullPlainOnce() throws Exception {
        if (pullFiles.length == 0) {
            throw new UnknownFormatFlagsException("Error: Wrong Watch File List format");
        }

        for(String file : pullFiles){
            String fileName = file.substring(file.lastIndexOf('/') + 1);
            bakFile(fileName);
            //get fileName's content
            String content = new String(client.getData().forPath( "/" + fileName));
            //according to the Change Mode,choose suitable function
            if (cmode == CHANGEMODE.OVERWRITE){
                overwriteChildDataToLocalFile(file,content);
            }else if(cmode == CHANGEMODE.APPEND){
//                appendChildDataToLocalFileBak(content,file);
                appendChildDataToLocalFile(content,file);
            }
            logger.info("Succeed! Znode /"+ client.getNamespace()+"/"+fileName + " has been pulled to " + file);
        }
    }

    private void bakFile(String filePath) throws IOException {
        if(new File(filePath).exists()){
            FileUtils.copyFile(new File(filePath),new File(filePath+".bak"+new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())));
        }
    }

    /**
     * Append different data from childData to local file.
     *
     * @param childData The data of childData
     * @param localFilePath The local file path
     *
     * */
    public void appendChildDataToLocalFile (String childData, String localFilePath) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Map<String,String> cMap = new LinkedHashMap<String, String>();
        String[] childLineContentArray = childData.split("\\n");
        for(String clc : childLineContentArray){
            if(!clc.startsWith("#")) {
                String[] cSplits = clc.split(" +");
                if (cSplits.length == 2) {
                    String ip = cSplits[0];
                    String host = cSplits[1].replace("\r", "");
                    cMap.put(ip, host);
                }
            }
        }
        File localFile = new File(localFilePath);
        List<String> localLineContentList = new ArrayList<String>();
        StringBuilder localFileContent = new StringBuilder();
        Map<String,String> lMap = new LinkedHashMap<String, String>();
        try {
            localLineContentList = FileUtils.readLines(localFile,"utf-8");
            localFileContent.append(FileUtils.readFileToString(localFile,"UTF-8"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        for(String llc : localLineContentList){
            if(!llc.startsWith("#")) {
                String[] cSplits = llc.split(" +");
                if (cSplits.length == 2) {
                    String ip = cSplits[0];
                    String host = cSplits[1];
                    lMap.put(ip, host);
                }
            }
        }
        boolean flag = true;
        for(String childIp : cMap.keySet()){
            if(lMap.containsKey(childIp)){
                if(! lMap.get(childIp).equals(cMap.get(childIp))){
                    // update
                    int startIndex = localFileContent.indexOf(childIp);
                    int endIndex = localFileContent.indexOf("\n",startIndex);
                    localFileContent = localFileContent.replace(startIndex,endIndex,"# Update hostname\n"
                            + childIp + " " + cMap.get(childIp));
                }
            }else{
                if(flag){
                    localFileContent.append("\n#-- [").append(sdf.format(System.currentTimeMillis())).append("] Add new hostname --");
                    flag = false;
                }
                localFileContent.append("\n").append(childIp).append(" ").append(cMap.get(childIp));
            }
            try {
                FileUtils.writeStringToFile(localFile, localFileContent.toString(), "UTF-8");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
    /**
     * Append different data from childData to local file.
     *
     * @param childData The data of childData
     * @param localFilePath The local file path
     *
     * */
    public void appendChildDataToLocalFileBak (String childData, String localFilePath) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        File localFile = new File(localFilePath);
        String[] ChildDataSplits = childData.split("\\n");
        List<String> localFileData = new ArrayList<String>();
        try {
            localFileData = FileUtils.readLines(localFile,"utf-8");
            FileUtils.writeStringToFile(localFile, "\n#-- [" + sdf.format(System.currentTimeMillis()) + "] Add new hostname --\n", "UTF-8", true);

        } catch (IOException e) {
            e.printStackTrace();
        }

        for(String split : ChildDataSplits){
            // if local file does not contain childData and not start with "#", appending childData to local file.
            if (!localFileData.contains(split) && !split.startsWith("#")){
                try {
                    FileUtils.writeStringToFile(localFile, split + "\n", "UTF-8", true);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

    }

    /**
     * Overwrite different data from childData to local file.
     *
     * @param file The local file path
     * @param content The data of znode childData
     * @throws Exception IOException
     */
    public void overwriteChildDataToLocalFile(String file, String content) throws Exception {
        FileUtils.writeStringToFile(new File(file),content, "UTF-8");
    }

}
