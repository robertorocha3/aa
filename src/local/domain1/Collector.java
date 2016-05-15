package local.domain1;

import gnu.getopt.Getopt;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.jsoup.Connection;
import org.jsoup.Jsoup;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.DecimalFormat;
import java.text.Normalizer;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Collector {
    boolean verbose,generateCharts,longerAnalysis;

    //String coreFile = "files/json/core_test.json",baseFile = "files/json/base_selection.json";
    String coreFile = "files/json/core_selection_long.json",baseFile = "files/json/base_selection_long.json";

    int numberOfRecords,numberOfFiles,numberOfMatches,succeededDownloads,numberOfFIleFields,literalMatches;
    int leaveOnlyNumbersAndLetters, removeMultipleSpaces,replaceNewLinesWithSpaces,removeLeadingAndTrailingSpaces,runid,statusCode;

    String e_literal,e_removeMultipleSpaces,e_leaveOnlyNumbersAndLetters,e_replaceNewLinesWithSpaces,e_removeLeadingAndTrailingSpaces, existingFile;
    String smallestNonMatch,rep,statusMessage,feedback;

    ArrayList<String> allExt,successExt,allUrlDomains,successUrlDomains,matchFieldsRecord;
    ArrayList<String> matchFieldsFile,downloadErrorCodes,TitleIDs,UrlIDs,IdIDs,fieldList;
    MultiValueMap extensionsAndDomains,domainsAndExtensions,successfulExtensionsAndDomains,successfulDomainsAndExtensions,matches;
    HashSet<String> urlAddressList;

    SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss, dd-MM-yyyy");
    Long startTime,finishTime;

    Db db;

    Logger logger;

    public static void main(String[] args) {
        new Collector(args);
    }

    public Collector(String[] args){

        verbose=false;
        generateCharts =false;
        longerAnalysis = false;
        allExt = new ArrayList();
        successExt = new ArrayList();
        allUrlDomains = new ArrayList();
        successUrlDomains = new ArrayList();
        matchFieldsRecord = new ArrayList();
        matchFieldsFile = new ArrayList();
        downloadErrorCodes = new ArrayList();
        TitleIDs = new ArrayList();
        UrlIDs = new ArrayList();
        IdIDs = new ArrayList();
        fieldList = new ArrayList();
        urlAddressList = new HashSet<>();
        extensionsAndDomains = new MultiValueMap();
        domainsAndExtensions = new MultiValueMap();
        successfulExtensionsAndDomains = new MultiValueMap();
        successfulDomainsAndExtensions = new MultiValueMap();
        matches = new MultiValueMap();
        sdf.setTimeZone(TimeZone.getTimeZone("GMT+12"));
        sdf.setLenient(false);
        e_literal ="n";
        e_removeMultipleSpaces ="n";
        e_leaveOnlyNumbersAndLetters ="n";
        e_replaceNewLinesWithSpaces ="n";
        e_removeLeadingAndTrailingSpaces ="n";
        db = new Db();

        Getopt g = new Getopt("Collector", args, "vcr:l"); // the colon (:) means that the option letter preceding it requires an argument
        int c;
        String arg = "";
        while ((c = g.getopt()) != -1){
            switch(c){
                case 'v':
                    System.out.println("Verbose mode selected.");
                    verbose = true;
                    break;
                case 'c':
                    generateCharts = true;
                    break;
                case 'r':
                    arg = g.getOptarg();
                    break;
                case 'l':
                    longerAnalysis = true;
                    break;
                default:
                    System.out.println("Program syntax: Collector -r <core|base|both> -v -c -l <y|n>");
                    System.out.println("-r options (mandatory):");
                    System.out.println("    core: collect and analyze data from http://core.ac.uk/");
                    System.out.println("    base: collect and analyze data from http://base-search.net/");
                    System.out.println("    both: collect and analyze data from both repositories");
                    System.out.println("-v (optional) = verbose mode");
                    System.out.println("-c (optional) = generate charts at the end");
                    System.out.println("-l (optional) = longer analysis (more or fewer files to download).The default is n (no).");
                    break;
            }
        }
        if (arg.equals("core")){
            this.getCoreFiles();
        }else if (arg.equals("base")){
            this.getBaseFiles();
        }else if (arg.equals("both")){
            //this.getBaseFiles();
            //this.getCoreFiles();
            //getURLsFromPage("http://digitalscholarship.unlv.edu/award/24");
            //getURLsFromPage("http://discovery.ucl.ac.uk/1298244/");
            args[1] = "base";
            new Thread(new RunIt(args)).start();
            this.getCoreFiles();

            //args[1] = "core";
            //new Thread(new RunIt(args)).start();
        }
    }
    public class RunIt implements Runnable {
        //private Logger logger = LoggerFactory.getLogger(Collector.class);
        private String[] args;
        public RunIt(String[] args) {
            this.args = args;
        }
        public void run() {
            try {   // this is to give some time for the other thread to write its first record and create its id in
                // the db so this thread can get the next id instead of the same one (if both run at the same time)
                Thread.sleep(10000); // 8000 also works but I am placing 10000 just in case if it is ran by a slower computer
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            new Collector(args);
        }
    }

    private void initLog(String rep){   // set up logging...
        logger = Logger.getLogger("fullrun_"+rep+"_"+runid);
        Properties prop = new Properties();
        prop.setProperty("log4j.logger.fullrun_"+rep+"_"+runid,"DEBUG, WORKLOG");
        prop.setProperty("log4j.appender.WORKLOG","org.apache.log4j.FileAppender");
        prop.setProperty("log4j.appender.WORKLOG.File", "files/logs/fullrun_"+rep+"_"+runid+".log");
        prop.setProperty("log4j.appender.WORKLOG.layout","org.apache.log4j.PatternLayout");
        prop.setProperty("log4j.appender.WORKLOG.layout.ConversionPattern","%d %c{1} - %m%n");
        prop.setProperty("log4j.appender.WORKLOG.Threshold","INFO");
        PropertyConfigurator.configure(prop);
        //logger.info("info test id=" + id);
        //logger.error("error test id=" + id);
    }

    private void getBaseFiles() {
        runid = db.getNumberFromDB("runID",0);
        initLog("base"); // runid has to be set first
        rep = "Base";
        resetCounters();
        String identification="",urlAddress = "";
        JSONObject mainObject;
        JSONArray array;
        startTime = System.currentTimeMillis();
        try {
            logger.info("\n---------------------------------------- Getting the BASE files (run ID: "+runid+")...\n");
            logger.info("Verbose mode: "+verbose);
            logger.info("Generate charts: "+generateCharts);
            logger.info("Start time: "+sdf.format(new Date(startTime)));
            mainObject = new JSONObject(new JSONTokener(new FileReader(baseFile)));
            //baseObject = new JSONObject(new JSONTokener(new FileReader(new String (baseFile.getBytes("UTF-8"), Charset.forName("UTF-8")))));
            array = (JSONArray) mainObject.get("records");

            for (int i = 0; i < array.length(); i++) {
                mainObject = (JSONObject) array.getJSONObject(i).get("record");
                //logger.info("baseobject: "+baseObject.toString());
                if (mainObject.has("title")) {
                    identification = mainObject.getString("title");
                }else if(mainObject.has("description")){
                    identification = mainObject.getString("description");
                }else{
                    identification = mainObject.getJSONArray("subjects").getString(0);
                }
                logger.info("\n---------------------------------------- Working on record # " +(numberOfRecords+1)+ " from BASE ----------------------------------------");
                findUrls(mainObject);
                logger.info("");
                if (!urlAddressList.isEmpty()){ // download the file(s)...
                    Iterator it = urlAddressList.iterator();
                    for (int u =0;u<urlAddressList.size();u++) {
                        urlAddress = it.next().toString();
                        preDownload(
                                "files/base/",
                                new URL(urlAddress).getHost(),
                                urlAddress.substring(urlAddress.lastIndexOf(".") + 1),
                                urlAddress,
                                fieldList,
                                urlAddress.substring(urlAddress.lastIndexOf("/") + 1),
                                mainObject,
                                identification
                        );
                    }
                } else {
                    preDownload("","none","---","",fieldList,"none",mainObject,identification);
                }
                numberOfRecords++;
                urlAddressList.clear();
                fieldList.clear();
            }
        } catch (Exception e) {
            logger.error("ERROR:",e);
        }
        finishTime = System.currentTimeMillis();
        showSummary();
    }

    private void getCoreFiles() {
        runid = db.getNumberFromDB("runID", 0);
        initLog("core"); // runid has to be set first
        String apiKey = "lWIs56bFaqSZ0cD8PLYdHg3OGUxyXVBK"; // TODO: remove this key when distributing the code
        rep = "Core";
        resetCounters();
        String jsonline = "";
        JSONObject mainObject;
        JSONArray array;
        String file,ext;
        Object identification = "";
        String urlAddress;
        startTime = System.currentTimeMillis();
        try {
            logger.info("\n---------------------------------------- Getting the CORE files (run ID: "+runid+")...\n");
            logger.info("Verbose mode: " + verbose);
            logger.info("Generate charts: " + generateCharts);
            logger.info("Start time: " + sdf.format(new Date(startTime)));
            BufferedReader reader = new BufferedReader(new FileReader(coreFile));
            while ((jsonline = reader.readLine()) != null) { // line (individual 10-record list)
                mainObject = new JSONObject(jsonline);
                array = (JSONArray) mainObject.get("data");
                for (int i = 0; i < array.length(); i++) { // record
                    mainObject = array.getJSONObject(i);
                    fieldList = sortList(mainObject.keys());
                    logger.info("---------------------------------------- Working on record # " + (numberOfRecords + 1) + " from CORE ----------------------------------------\n");
                    findUrls(mainObject);
                    identification = mainObject.getString(fieldList.get(0));
                    urlAddressList.add("https://core.ac.uk:443/api-v2/articles/get/" + identification + "/download/pdf?apiKey=" + apiKey);
                    Iterator it = urlAddressList.iterator();
                    for (int u = 0; u < urlAddressList.size(); u++) {
                        urlAddress = it.next().toString();
                        if (urlAddress.contains("core.ac.uk:443")){
                            file = identification+".pdf";
                            ext = "pdf";
                        }else{
                            file = urlAddress.substring(urlAddress.lastIndexOf("/") + 1);
                            ext = urlAddress.substring(urlAddress.lastIndexOf(".") + 1);
                        }
                        //logger.info("fieldList.size: "+fieldList.size());
                        preDownload(
                                "files/core/",
                                new URL(urlAddress).getHost(),
                                ext,
                                urlAddress,
                                fieldList,
                                file,
                                mainObject,
                                (String)identification
                        );
                    }
                    numberOfRecords++;
                    urlAddressList.clear();
                    fieldList.clear();
                }
            }
            finishTime = System.currentTimeMillis();
            showSummary();
        }catch(Exception e){
            //Logger.getLogger(Collector.class.getName()).log(Level.SEVERE, null, e);
            logger.error("ERROR:",e);
        }
    }

    private void findUrls(JSONObject mainObject) throws JSONException, MalformedURLException {    // find where the URL is in the record...
        String key,innerkey,urlAddress = "";
        Iterator<String> keys = mainObject.keys(),innerkeys;
        Object value;
        while (keys.hasNext()) {
            key = keys.next();
            if (rep.equals("Base")){
                fieldList.add(key);
                countIDs(key);
            }
            value = mainObject.get(key);
            if (value instanceof JSONArray) {
                for(int v=0;v<((JSONArray) value).length();v++) {
                    //fieldList.add(key);
                    //countIDs(key);
                    urlAddress = checkUrl(((JSONArray) value).getString(v));
                    if (!urlAddress.equals("")){
                        logger.info("Found a file named \""+((JSONArray) value).getString(v)+"\" in the \""+key+"\" key");
                        urlAddressList.add(urlAddress);
                    }else{
                        if (longerAnalysis) {
                            getURLsFromPage(((JSONArray) value).getString(v));
                        }
                    }
                }
            }else if (value instanceof JSONObject) {
                innerkeys = ((JSONObject) value).keys();
                while (innerkeys.hasNext()){
                    innerkey = innerkeys.next();
                    fieldList.add(innerkey);
                    countIDs(innerkey);
                    urlAddress = checkUrl(((JSONObject) value).getString(innerkey));
                    if (!urlAddress.equals("")){
                        logger.info("Found a file named \""+((JSONObject) value).getString(innerkey)+"\" in the \""+key+"\" key");
                        urlAddressList.add(urlAddress);
                    }else{
                        if (longerAnalysis) {
                            getURLsFromPage(((JSONObject) value).getString(innerkey));
                        }
                    }
                }
            }else if(value instanceof String){
                //fieldList.add(key);
                //countIDs(key);
                urlAddress = checkUrl((String) value);
                if (!urlAddress.equals("")){
                    logger.info("Found a file named \""+value+"\" in the \""+key+"\" key");
                    urlAddressList.add(urlAddress);
                }else{
                    if (longerAnalysis) {
                        getURLsFromPage((String) value);
                    }
                }
            }
        }
        //countIDs(fieldList);
    }

    private void countIDs(String field){
        if (field.contains("title")) {
            TitleIDs.add(field);
        } else if (field.contains("url")) {
            UrlIDs.add(field);
        } else if (field.equalsIgnoreCase("id")) {
            IdIDs.add(field);
        }
    }
    /*
    private void countIDs(ArrayList fieldList){
        String key="";
        for (int f=0;f<fieldList.size();f++){
            key=fieldList.get(f).toString().toLowerCase();
            if (key.contains("title")) {
                TitleIDs.add(key);
            } else if (key.contains("url")) {
                UrlIDs.add(key);
            } else if (key.equalsIgnoreCase("id")) {
                IdIDs.add(key);
            }
        }
    }
    */
    private String checkUrl(String value) throws MalformedURLException {
        String urlAddress = "";
        Pattern p = Pattern.compile("\\b(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]",Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(value);
        if (m.find()){  // if the string has a url, change its value to the url's address...
            value=m.group(0);
            String testvalue = value.toLowerCase(); // url
            if ((testvalue.endsWith(".pdf")) ||
                    (testvalue.endsWith(".jpg")) ||
                    (testvalue.endsWith(".doc")) ||
                    (testvalue.endsWith(".docx")) ||
                    //(testvalue.endsWith(".xml")) ||
                    //(testvalue.endsWith(".gif")) ||
                    (testvalue.endsWith(".tif")) ||
                    (testvalue.endsWith(".wav")) ||
                    (testvalue.endsWith(".gz")) ||
                    (testvalue.endsWith(".zip")) ||
                    (testvalue.endsWith(".xls")) ||
                    (testvalue.endsWith(".ppt")) ||
                    (testvalue.endsWith(".pptx"))) {
                urlAddress = value;
                String extension = urlAddress.substring(urlAddress.lastIndexOf(".")+1);
                String domain = new URL(urlAddress).getHost();
                allExt.add(extension);
                allUrlDomains.add(domain);
                extensionsAndDomains.put(extension,domain);
                domainsAndExtensions.put(domain,extension);
            }
        }
        return urlAddress;
    }

    private void getURLsFromPage(String urlAddress){
        Connection.Response conn = null;
        try {    // check if the file is ready to be downloaded...
            conn = Jsoup.connect(urlAddress)
                    .userAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.21 (KHTML, like Gecko) Chrome/19.0.1042.0 Safari/535.21")
                    .timeout(10000)
                    .ignoreContentType(true)
                    .followRedirects(true)
                    .ignoreHttpErrors(true)
                    .execute();
        } catch (Exception e) {}
        if (conn!=null){
            String[] lines = conn.body().split("\n");
            String url = "";
            try {
                for(String line:lines){
                    url = checkUrl(line);
                    if (!url.equals("")){
                        logger.info("Found url \""+url+"\" in page "+urlAddress);
                        urlAddressList.add(url);
                    }
                }
            } catch (MalformedURLException e) {
                logger.error("ERROR:",e);
            }
        }
    }

    private void preDownload(String destPath,String domain,String extension,String urlAddress,ArrayList fieldList,String fileName,JSONObject mainObject,String identification){
        boolean hasFile=false;
        long fileSize = 0;
        String success = "";
        long metaTime = 0;
        long downStartTime = 0;
        long downFinishTime = 0;
        File file = null;
        feedback = "";
        boolean downloadPerformed = false;
        //if (shouldBeDownloaded(0,destPath,fileName,null)) {   // this would defeat the purpose of comparing the last modification date of the remote file with the local one's
            if (!urlAddress.equals("")) {                       // but makes the run a lot faster if all or many of the files have already been downloaded
                logger.info("Starting download process for url: " + urlAddress + "...");
                allUrlDomains.add(domain);
                allExt.add(extension);
                extensionsAndDomains.put(extension, domain);
                domainsAndExtensions.put(domain, extension);
                Connection.Response conn = null;
                try {    // check if the file is ready to be downloaded...
                    conn = Jsoup.connect(urlAddress)
                            .userAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.21 (KHTML, like Gecko) Chrome/19.0.1042.0 Safari/535.21")
                            .timeout(10000)
                            .ignoreContentType(true)
                            .followRedirects(true)
                            .ignoreHttpErrors(true)
                            .execute();
                } catch (Exception e) {
                }
                try {
                    if (conn != null) {  // if the url is available...
                        statusCode = conn.statusCode();
                        statusMessage = conn.statusMessage();
                        downStartTime = System.currentTimeMillis();
                        if (statusCode == 200) {    // if the file is downloadable...
                            String remoteFileLastModDate = conn.headers().get("Last-Modified");
                            if (remoteFileLastModDate == null) { // if the remote file's http head doesn't have a Last-Modified field, download the file anyway (it will overwrite the file if it already exists)
                                logger.info("Could not determine the last time the remote file was modified. Downloading it anyway...");
                                performDownload(destPath, fileName, urlAddress);
                                logger.info("Download status message: " + statusMessage + "\n");
                                downloadPerformed = true;
                            } else {
                                logger.info("Remote file's last modification date: " + remoteFileLastModDate);
                                if (shouldBeDownloaded(new SimpleDateFormat("E, d MMM yyyy HH:mm:ss Z", Locale.ENGLISH).parse(remoteFileLastModDate).getTime() / 1000, destPath, fileName, conn.headers().get("Content-Length"))) {
                                    performDownload(destPath, fileName, urlAddress);
                                    logger.info("Download status message: " + statusMessage + "\n");
                                    downloadPerformed = true;
                                }
                            }
                        } else {
                            logger.info("");
                            logger.info("** Could not download the file! **");
                            logger.info("Found a " + statusCode + " error when retrieving file named " + fileName);
                            logger.info("Failed url: " + urlAddress);
                            logger.info("Download status message: " + statusMessage + "\n");
                            downloadErrorCodes.add(Integer.toString(statusCode));
                        }
                        downFinishTime = System.currentTimeMillis();
                        if (downloadPerformed) {
                            logger.info("Download duration: " + getDurationString(downFinishTime - downStartTime) + "\n");
                        }
                    } else {
                        logger.info("The url \"" + urlAddress + "\" could not be resolved!");
                    }

                    file = new File(destPath + fileName);
                    if (file.exists()) {
                        if (validateThis(destPath + fileName).equals("valid")) { // perform the post-download validation
                            success = "y";
                        } else {
                            success = "n";
                        }
                    } else {
                        this.getFileInfo(destPath + fileName);
                        success = "n";
                        fileSize = 0;
                    }

                    if (success.equals("y")) {   // don't perform the analysis if there's no valid file
                        logger.info("Download completed successfully!");
                        fileSize = file.length();
                        metaTime = System.currentTimeMillis();
                        analyzeMetadata(mainObject, destPath + fileName, fieldList, identification);
                        metaTime = System.currentTimeMillis() - metaTime;
                        logger.info("Comparison duration: " + getDurationString(metaTime) + "\n");
                        succeededDownloads++;
                        successExt.add(extension);
                        successUrlDomains.add(domain);
                        successfulExtensionsAndDomains.put(extension, domain);
                        successfulDomainsAndExtensions.put(domain, extension);
                    } else {
                        logger.info("Download failed.");
                    }
                    addToDB("addDownload", new String[]{
                            Integer.toString(numberOfRecords + 1),
                            fileName,
                            urlAddress,
                            String.valueOf(fileSize),
                            domain,
                            String.valueOf(statusCode),
                            statusMessage,
                            success,
                            String.valueOf(downStartTime),
                            String.valueOf(downFinishTime),
                            extension,
                            existingFile
                    });
                    numberOfFiles++;
                } catch (Exception e) {
                    logger.error("ERROR:", e);
                }
            } else {
                logger.info("\nThe record named \"" + identification + "\" does not have a file to be downloaded...");
            }
        //}
        addToDB("addRecord", new String[]{
                Integer.toString(numberOfRecords + 1),
                fileName,
                urlAddress,
                domain,
                extension,
                String.valueOf(fieldList.size()),
                String.valueOf(numberOfFIleFields),
                String.valueOf(metaTime)
        });
        resetPredownloadFields();
    }

    private void performDownload(String destPath,String fileName,String urlAddress){    // perform the actual download...
        try {
            logger.info("\nDownloading file named \"" + fileName +" from "+ urlAddress+"\"...");
            URL url = new URL(urlAddress);
            InputStream in = url.openStream();
            Files.copy(in, Paths.get(destPath+fileName), StandardCopyOption.REPLACE_EXISTING);
            in.close();
        } catch (Exception e) {
            logger.error("ERROR:",e);
        }
    }

    private boolean shouldBeDownloaded(long httpLastModDate, String destPath, String fileName, String length){
        boolean response = false;
        File file = new File(destPath+fileName);
        logger.info("");
        if (file.exists()) {
            if (length!=null){
                if (!Long.toString(file.length()).equals(length)){
                    logger.info("The remote file has a different size ("+length+") than the local one's ("+file.length()+").");
                    response = true;
                }else{
                    if (!validateThis(destPath+fileName).equals("valid")){   // pre-download validation...
                        response = true;
                    }
                }
            }
            logger.info("The file "+fileName+" has already been downloaded.");
            long fileLastModDate = file.lastModified()/1000;
            if (verbose) {
                logger.info("last time the remote file was modified (unix format): " + httpLastModDate);
                logger.info("Last time the local file was modified: " + fileLastModDate);
            }
            if (httpLastModDate!=0) {
                if (httpLastModDate > fileLastModDate) {
                    logger.info("The remote file is newer than the local one.");
                    response = true;
                } else {
                    existingFile = "y";
                }
            }
        }else{
            logger.info("The file "+fileName+" has not been downloaded yet.");
            response = true;
        }
        return response;
    }

    private String validateThis(String filename){
        long startTime = System.currentTimeMillis();
        feedback = "not valid";
        HashMap<String, String> map = new HashMap<String,String>();
        //map.put("gif","GIF-hul");
        map.put("gz","GZIP-kb");
        map.put("jpg","JPEG-hul");
        map.put("pdf","PDF-hul");
        map.put("tif","TIFF-hul");
        map.put("wav","WAVE-hul");
        //map.put("xml","XML-hul");
        Map.Entry entry;
        Set mapSet = map.entrySet();
        Iterator mapIterator = mapSet.iterator();
        String fileKey = "",fileValue = "",module = "";
        logger.info("Validating file named: "+filename);
        while (mapIterator.hasNext()) {
            entry = (Map.Entry) mapIterator.next();
            fileKey = (String) entry.getKey();
            fileValue = (String) entry.getValue();
            if (filename.endsWith(fileKey)){
                module = fileValue;
            }
            if (verbose){
                logger.info("key: "+fileKey);
                logger.info("value: "+fileValue);
            }
        }
        logger.info("Using module: "+module);
        if (!module.equals("")) {
            try {
                Process p = Runtime.getRuntime().exec("jhove.bat -m " + module+" "+filename);
                p.waitFor(2, TimeUnit.SECONDS);
                BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
                String line = "";
                if (reader.ready()) {
                    while ((line = reader.readLine()) != null) {
                        if (line.contains("SignatureMatches:")){
                            if (reader.readLine().contains(module)){
                                feedback = "valid";
                            }
                            break;
                        }
                    }
                }
                reader.close();
                p.destroy();
            } catch (Exception e) {
                logger.error("ERROR:",e);
            }
        }else{
            feedback = "not found";
        }
        logger.info("Validation duration: "+getDurationString(System.currentTimeMillis()-startTime));
        logger.info("Status: "+feedback+"\n");
        return feedback;
    }

    private String getDurationString(Long duration){
        String durationString = "";
        long days = (duration / (24 * 60 * 60 * 1000));
        long hours = (duration / (60 * 60 * 1000) % 24);
        long minutes =(duration / (60 * 1000) % 60);
        long seconds = (duration / 1000 % 60);
        long milliseconds = (duration % 1000);
        if (days>0){
            durationString+=Long.toString(days)+" day(s),";
        }
        if (hours>0){
            durationString+=Long.toString(hours)+" hour(s),";
        }
        if (minutes>0){
            durationString+=Long.toString(minutes)+" minute(s),";
        }
        if (seconds>0){
            durationString+=Long.toString(seconds)+" second(s) and ";
        }
        if (milliseconds>0){
            durationString+=Long.toString(milliseconds)+" millisecond(s)";
        }
        return durationString;
    }

    // get the matadata from the file then compare it with the record's using the compareRecord method
    private void analyzeMetadata(JSONObject mainObject, String file, ArrayList fieldList, String title) throws JSONException {
        //verbose = true;
        JSONArray array;
        JSONObject object;
        boolean hasaMatch=false;
        Iterator<String> keys;
        String mainkey="";
        String key="";
        HashMap fileFields = this.getFileInfo(file); // the extraction is made here in order to avoid it from being done multiple times in compareRecord()
        logger.info("Comparing the record's metadata with the one from the file named "+file);
        for (int f = 0; f < fieldList.size(); f++) {    // cycle through the record
            mainkey = (String) fieldList.get(f);
            if (verbose) {
                logger.info("mainkey: "+mainkey);
            }
            if (mainObject.has(mainkey)) {
                if (mainObject.get(mainkey) instanceof JSONObject) {
                    object = mainObject.getJSONObject(mainkey);
                    keys = object.keys();
                    while (keys.hasNext()) {
                        key = keys.next();
                        if (verbose) {
                            logger.info("key: " + mainkey + " and value: " + object.getString(key));
                        }
                        hasaMatch=compareRecord(mainkey+":"+key, object.getString(key),file, title,fileFields);
                    }
                } else if (mainObject.get(mainkey) instanceof JSONArray) {
                    array = mainObject.getJSONArray(mainkey);
                    if (verbose) {
                        logger.info("size: "+array.length());
                    }
                    for (int a = 0; a < array.length(); a++) {
                        if (verbose) {
                            logger.info("key: "+mainkey+" and value: "+array.getString(a));
                        }
                        hasaMatch=compareRecord(mainkey, array.getString(a),file, title,fileFields);
                    }
                }else if(mainObject.get(mainkey) instanceof String){
                    hasaMatch=compareRecord(mainkey, mainObject.getString(mainkey),file, title,fileFields);
                }
            }
        }
        if (!hasaMatch){
            logger.info("");
            logger.info("No matches were found.");
        }
        //verbose = false;
    }

    // get a single key and record pair from a record and compare with the metadata of its respective file
    private boolean compareRecord(String recordKey,String recordValue,String filename,String title, HashMap fields){
        Map.Entry entry;
        boolean matchFound=false;
        Set mapSet = fields.entrySet();
        Iterator mapIterator = mapSet.iterator();
        String fileKey = "",fileValue = "",matchReasons = "";
        while (mapIterator.hasNext()){  // each record field is compared to all fields of the file
            entry = (Map.Entry) mapIterator.next();
            fileKey = (String) entry.getKey();
            fileValue = (String) entry.getValue();
            if (verbose) {
                logger.info("filekey: " + fileKey + " and filevalue: " + fileValue);
                logger.info("recordKey: " + recordKey + " and recordValue: " + recordValue);
            }
            matchReasons+=findAMatch(recordValue,fileValue,recordValue,"literalMatches");
            matchReasons+=findAMatch(recordValue.replaceAll("\\s+"," "),fileValue.replaceAll("\\s+"," "),recordValue,"removeMultipleSpaces");
            matchReasons+=findAMatch(recordValue.trim(),fileValue.trim(),recordValue,"removeLeadingAndTrailingSpaces");
            matchReasons+=findAMatch(recordValue.replaceAll("\\n"," "),fileValue.replaceAll("\\n"," "),recordValue,"replaceNewLinesWithSpaces");
            matchReasons+=findAMatch(Normalizer.normalize(recordValue, Normalizer.Form.NFD).replaceAll("[^\\p{L}\\p{Nd}]+",""),
                    Normalizer.normalize(fileValue, Normalizer.Form.NFD).replaceAll("[^\\p{L}\\p{Nd}]+",""),recordValue,"leaveOnlyNumbersAndLetters");
            if (!matchReasons.equals("")){
                logger.info("");
                logger.info("*** MATCH FOUND ***");
                logger.info("Key \"" + recordKey + "\" from record titled \"" + title + "\" ");
                logger.info("matches key \"" + fileKey+"\" from file \""+filename+"\"");
                logger.info("--- value from record: \""+recordValue+"\"");
                logger.info("--- value from file: \""+fileValue+"\"");
                logger.info(matchReasons);
                matchReasons="";
                numberOfMatches++;
                addToDB(
                        "addMatch",
                        new String[]{
                                recordKey,
                                fileKey,
                                recordValue,
                                fileValue,
                                Integer.toString(numberOfRecords+1),
                                filename.replace("files/"+rep.toLowerCase()+"/",""),
                                e_literal,
                                e_removeMultipleSpaces,
                                e_leaveOnlyNumbersAndLetters,
                                e_replaceNewLinesWithSpaces,
                                e_removeLeadingAndTrailingSpaces
                        }
                );
                matchFieldsRecord.add(recordKey.toLowerCase());
                matchFieldsFile.add(fileKey.toLowerCase());
                matchFound = true;
                resetMatchEnums();
            }
        }
        return matchFound;
    }

    private String findAMatch(String value1, String value2, String originalRecordValue,String checkType){
        String matchMessage = "";
        if (checkType.equals("literalMatches")) { // compare the two fields literally
            if (value1.equals(value2)){
                matchMessage="Match found by comparing both literally\n";
                e_literal ="y";
                literalMatches++;
            }else{
                checkSmallestNonMatch(originalRecordValue);
            }
        }else{
            if (value1.equalsIgnoreCase(value2)){
                if(checkType.equals("removeMultipleSpaces")){ // compare the two fields by replacing one or more spaces by a single space in a case insensitive manner
                    matchMessage="Match found by removing multiple spaces\n";
                    e_removeMultipleSpaces ="y";
                    removeMultipleSpaces++;
                }else if(checkType.equals("removeLeadingAndTrailingSpaces")){ // compare the two fields by removing leading and trailing spaces
                    matchMessage="Match found by removing leading and trailing spaces\n";
                    e_removeLeadingAndTrailingSpaces ="y";
                    removeLeadingAndTrailingSpaces++;
                }else if(checkType.equals("replaceNewLinesWithSpaces")){ // compare the two fields by replacing new lines with spaces
                    matchMessage="Match found by replacing new lines with spaces\n";
                    e_replaceNewLinesWithSpaces ="y";
                    replaceNewLinesWithSpaces++;
                }else if(checkType.equals("leaveOnlyNumbersAndLetters")){ // compare the two fields by removing all the non-letters and non-numbers in a case insensitive manner
                    matchMessage="Match found by leaving only numbers and letters on both fields\n";
                    e_leaveOnlyNumbersAndLetters ="y";
                    leaveOnlyNumbersAndLetters++;
                }
            }else{
                checkSmallestNonMatch(originalRecordValue);
            }
        }
        if (!matchMessage.equals("")){
            matches.put(value1,value2);
        }
        return matchMessage;
    }

    private void checkSmallestNonMatch(String recordValue){
        if (recordValue.length()!=0) {
            if (recordValue.length() < smallestNonMatch.length()) {
                smallestNonMatch = recordValue;
            }
        }
    }

    private HashMap getFileInfo(String pdfFile) {
        Process p;
        String key="";
        String value="";
        HashMap<String, String> map = new HashMap<String,String>();
        try {
            p = Runtime.getRuntime().exec("exiftool "+pdfFile);
            p.waitFor(2,TimeUnit.SECONDS);
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = "";
            if (reader.ready()) {
                while ((line = reader.readLine()) != null) {
                    key = line.split(":", 2)[0];
                    value = line.split(":", 2)[1];
                    if (verbose) {
                        logger.info("key: " + key + " and value: " + value);
                    }
                    map.put(key.replaceAll("\\s+$", ""), value.replaceFirst(" ", ""));
                    numberOfFIleFields++;
                }
            }
            reader.close();
            p.destroy();
        } catch (Exception e) {
            logger.error("ERROR:",e);
        }
        return map;
    }

    private ArrayList sortList(Iterator list){ // place the id at the top of the list
        ArrayList al = new ArrayList();
        al.add("id");
        Object obj;
        while (list.hasNext()){
            obj=list.next();
            if (!obj.equals("id")){
                al.add(obj.toString());
            }
        }
        return al;
    }

    private void resetCounters(){
        numberOfRecords = 0;
        numberOfFiles = 0;
        numberOfMatches = 0;
        succeededDownloads = 0;
        leaveOnlyNumbersAndLetters = 0;
        removeMultipleSpaces = 0;
        replaceNewLinesWithSpaces = 0;
        removeLeadingAndTrailingSpaces = 0;
        literalMatches = 0;
        resetPredownloadFields();
        allExt.clear();
        successExt.clear();
        allUrlDomains.clear();
        successUrlDomains.clear();
        extensionsAndDomains.clear();
        domainsAndExtensions.clear();
        successfulExtensionsAndDomains.clear();
        successfulDomainsAndExtensions.clear();
        matches.clear();
        matchFieldsRecord.clear();
        matchFieldsFile.clear();
        downloadErrorCodes.clear();
        TitleIDs.clear();
        UrlIDs.clear();
        IdIDs.clear();
        fieldList.clear();
        urlAddressList.clear();
        resetMatchEnums();

        smallestNonMatch = "";
        for(int s=0;s<1000;s++){
            smallestNonMatch+=" ";
        }
    }

    private void resetPredownloadFields(){
        numberOfFIleFields = 0;
        statusMessage = "FAILED";
        statusCode=0;
        existingFile = "n";
    }

    private void resetMatchEnums(){
        e_literal ="n";
        e_removeMultipleSpaces ="n";
        e_leaveOnlyNumbersAndLetters ="n";
        e_replaceNewLinesWithSpaces ="n";
        e_removeLeadingAndTrailingSpaces ="n";
    }

    private int counter(String subject, String collection){
        int c = 0;
        ArrayList<String> arrayList = new ArrayList();
        if (collection.equals("allExt")){
            arrayList = allExt;
        }else if(collection.equals("successExt")){
            arrayList = successExt;
        }else if(collection.equals("allUrlDomains")){
            arrayList = allUrlDomains;
        }else if(collection.equals("successUrlDomains")){
            arrayList =successUrlDomains;
        }else if(collection.equals("matchFieldsRecord")){
            arrayList = matchFieldsRecord;
        }else if(collection.equals("matchFieldsFile")){
            arrayList = matchFieldsFile;
        }else if(collection.equals("downloadErrorCodes")){
            arrayList = downloadErrorCodes;
        }else if(collection.equals("TitleIDs")){
            arrayList = TitleIDs;
        }else if(collection.equals("UrlIDs")){
            arrayList = UrlIDs;
        }else if(collection.equals("IdIDs")){
            arrayList = IdIDs;
        }
        for (int i = 0; i< arrayList.size(); i++){
            if (arrayList.get(i).equals(subject)){
                c++;
            }
        }
        return c;
    }

    private int successCounter(String subject,String subject2,String collection){
        MultiValueMap map = new MultiValueMap();
        int c = 0;
        if(collection.equals("successfulDomainsAndExtensions")){
            map = successfulDomainsAndExtensions;
        }else if(collection.equals("successfulExtensionsAndDomains")) {
            map = successfulExtensionsAndDomains;
        }
        Object[] keys = map.keySet().toArray();
        Object[] value =null;
        Object key = null;
        for (int k = 0; k< keys.length; k++){
            key = keys[k].toString();
            if (key.equals(subject)) {
                value = map.getCollection(key).toArray();
                for (int v = 0; v < value.length; v++) {
                    if (value[v].toString().equals(subject2)) {
                        c++;
                    }
                }
            }
        }
        return c;
    }
    private void displaySubjects(String checkThis, String subject,String item){ // it also prepares the graph data
        MultiValueMap map = new MultiValueMap();    // MultiValueMaps store repeated keys
        String smap = "";
        String subjectType = "";
        if (subject.equals("extension")) {  // here, the given value (checkThis) has to be an extension
            map = domainsAndExtensions; // domain is the key and the extension is the value
            smap = "successfulDomainsAndExtensions";
            subjectType = "link(s)";
        }else if (subject.equals("domain")){// here, the given value (checkThis) has to be a domain
            map = extensionsAndDomains; // the extension is the key and the domain is the value
            smap = "successfulExtensionsAndDomains";
            subjectType = "files(s)";
        }
        Object[] keys = map.keySet().toArray();
        Object[] value =null;
        Object key = null;
        int counter = 0;
        int successCounter=0;
        for (int i = 0; i< keys.length; i++){
            key = keys[i];
            value = map.getCollection(key).toArray();
            for (int v=0;v<value.length;v++){
                if (value[v].toString().equals(checkThis)){  // compares the array's value with the given one
                    counter++;
                }
            }
            if (counter>0) {
                successCounter = successCounter(key.toString(),checkThis,smap);
                logger.info("    "+key.toString()+" - "+counter+" "+subjectType+", ");
                logger.info("being "+successCounter+" successful download(s) and "+(counter-successCounter)+" failure(s)");
            }
            counter=0;
        }
    }

    private void showSummary(){
        logger.info("");
        logger.info("######################################################### Summary of findings - "+rep.toUpperCase()+" #########################################################");
        logger.info("");
        addToDB("addRun",null);
        logger.info("Start time: "+ sdf.format(startTime));
        logger.info("Finish time: "+sdf.format(finishTime));
        logger.info("Download duration: "+getDurationString(finishTime-startTime));
        logger.info("Total number of "+rep+" records: "+numberOfRecords);
        logger.info("");
        logger.info("-------- File information");
        logger.info("The number of downloaded files ("+ succeededDownloads +") makes "+
                new DecimalFormat("#.00").format(((double) succeededDownloads *100)/(double)numberOfFiles)+
                "% of the total number of "+rep+" download links available ("+numberOfFiles+").");
        logger.info("");
        logger.info("Of those links:");
        Object[] uniqueFields = new HashSet<Object>(allExt).toArray();
        int all = 0;
        int success = 0;
        String item = "";
        for (int i=0;i<uniqueFields.length;i++){
            all = counter(uniqueFields[i].toString(),"allExt");
            if (all>0) {
                success = counter(uniqueFields[i].toString(), "successExt");
                logger.info("");
                item = uniqueFields[i].toString().toUpperCase();
                logger.info(all + " had the " + item + " extension ("+
                        new DecimalFormat("#.00").format((((double)all*100)/(double)numberOfFiles))+"% of all links), being " + success +" downloaded successfully ("+
                        new DecimalFormat("#.00").format((((double)success*100)/(double) succeededDownloads))+"% of all downloads) which makes a "+
                        new DecimalFormat("#.00").format((((double)success*100)/(double) all))+"% success rate");
                logger.info("--- The following domains had this extension:");
                displaySubjects(uniqueFields[i].toString(),"extension",item);
            }
        }
        Charts thischart = new Charts();
        if (generateCharts){
            thischart.createBarChart(
                    "Successful domains per extension - "+rep.toUpperCase(),
                    "Domains",
                    "Successful downloads",
                    //thischart.pullBarChartData("domPerExtPerRun_1",new String[]{Integer.toString(runid)}),
                    "domPerExtPerRun_1",
                    finishTime,
                    Integer.toString(runid),
                    0,
                    true
            );
        }
        logger.info("");
        logger.info("Extensions from domains:");
        uniqueFields = new HashSet<Object>(allUrlDomains).toArray();
        for (int f=0;f<uniqueFields.length;f++){
            all = counter(uniqueFields[f].toString(),"allUrlDomains");
            if (all>0) {
                success = counter(uniqueFields[f].toString(), "successUrlDomains");
                logger.info("");
                item = uniqueFields[f].toString();
                logger.info("\"" + item + "\" counts " + all + " link(s) (" +
                        new DecimalFormat("#.00").format((((double) all * 100) / (double) numberOfFiles)) + "% of all links) and " + success + " download(s) (" +
                        new DecimalFormat("#.00").format((((double) success * 100) / (double) succeededDownloads)) + "% of all downloads) which makes a " +
                        new DecimalFormat("#.00").format((((double) success * 100) / (double) all)) + "% success rate");
                logger.info("--- This domain had the following extensions:");
                displaySubjects(uniqueFields[f].toString(),"domain",item);
            }
        }
        if (generateCharts){
            thischart.createBarChart(
                    "Successful extensions per domain - "+rep.toUpperCase(),
                    "Extensions",
                    "Successful downloads",
                    //thischart.pullBarChartData("extPerDomPerRun_1",new String[]{Integer.toString(runid)}),
                    "extPerDomPerRun_1",
                    finishTime,
                    Integer.toString(runid),
                    0,
                    true
            );
        }
        logger.info("");
        logger.info("Download error codes:");
        displayFieldCounters(downloadErrorCodes,"downloadErrorCodes","error(s)");
        logger.info("");
        logger.info("-------- Field information");
        int norf = db.getNumberFromDB("numberOfRecordFields",runid);
        int nom = db.getNumberFromDB("numberOfMatches",runid);
        int noff = db.getNumberFromDB("numberOfFIleFields",runid);
        logger.info("Number of fields analyzed on "+rep+" records: "+norf);
        logger.info("Number of fields analyzed on "+rep+" files: "+noff);
        logger.info("Number of matches found when comparing "+rep+" records with "+rep+" files: "+nom);
        logger.info("The number of matches ("+nom+") makes "+
                new DecimalFormat("#.00").format((((double)nom*100)/(double)norf))+
                "% of the total number of "+rep+" fields ("+norf+").");
        logger.info("");
        logger.info("The smallest non-match field is: "+smallestNonMatch);
        logger.info("");
        logger.info("--- Identifiers:");
        displayFieldCounters(TitleIDs,"TitleIDs","title identifiers");
        displayFieldCounters(UrlIDs,"UrlIDs","url identifiers");
        displayFieldCounters(IdIDs,"IdIDs","id identifiers");
        logger.info("");
        logger.info("--- Match methods:");
        logger.info("    By removing multiple spaces: "+ removeMultipleSpaces);
        logger.info("    By leaving only numbers and letters on both fields: "+leaveOnlyNumbersAndLetters);
        logger.info("    By replacing new lines with spaces: "+replaceNewLinesWithSpaces);
        logger.info("    By removing leading and trailing spaces: "+removeLeadingAndTrailingSpaces);
        logger.info("");
        logger.info("--- Reasons for the match(es) in the records:");
        displayFieldCounters(matchFieldsRecord,"matchFieldsRecord","match(es)");
        logger.info("");
        logger.info("--- Reasons for the match(es) in the files:");
        displayFieldCounters(matchFieldsFile,"matchFieldsFile","match(es)");
        logger.info("");
        logger.info("######################################################### End of the Summary - "+rep.toUpperCase()+" ##########################################################");
    }

    private void displayFieldCounters(ArrayList list, String listname, String typemessage){
        Object[] uniqueFields = new HashSet<Object>(list).toArray();
        int all = 0;
        for (int f=0;f<uniqueFields.length;f++){
            all = counter(uniqueFields[f].toString(),listname);
            if (all>0) {
                logger.info("    "+uniqueFields[f].toString()+" - "+all+" "+typemessage);
                if (listname.endsWith("IDs")){
                    addToDB("addId",new String[]{
                            uniqueFields[f].toString(),
                            Integer.toString(all),
                            typemessage.replace(" identifiers","")
                    });
                }
            }
        }
    }

    private void addToDB(String action, String[] array){
        try{
            java.sql.Connection conn = db.getConn();
            String query = "";
            if (action.equals("addRun")){
                query = "insert into rep_run (" +
                        "id," +
                        "repname," +
                        "startTime," +
                        "finishTime" +
                        ") values ("+
                            runid+",\""+
                            rep.toLowerCase()+"\","+
                            startTime+","+
                            finishTime+
                        ")";
            }else if (action.equals("addRecord")){
                query = "insert into rep_record (" +
                        "runId," +
                        "recordNumber," +
                        "fileName," +
                        "url," +
                        "domname," +
                        "extname," +
                        "numberOfRecordFields," +
                        "numberOfFIleFields," +
                        "analysisDuration" +
                        ") values (" +
                        runid + "," +
                        array[0] + ",\"" +
                        array[1] + "\",\"" +
                        array[2] + "\",\"" +
                        array[3] + "\",\"" +
                        array[4] + "\"," +
                        array[5] + "," +
                        array[6] + "," +
                        array[7] + ")";
            }else if (action.equals("addDownload")){
                query = "insert into rep_download (" +
                        "runId," +
                        "recordNumber," +
                        "fileName," +
                        "url," +
                        "fileSize," +
                        "domname," +
                        "statusCode," +
                        "statusMessage," +
                        "success," +
                        "startTime," +
                        "finishTime," +
                        "extname," +
                        "existingFile" +
                        ") values (" +
                        runid + "," +
                        array[0] + ",\"" +
                        array[1] + "\",\"" +
                        array[2] + "\"," +
                        array[3] + ",\"" +
                        array[4] + "\"," +
                        array[5] + ",\"" +
                        array[6] + "\",\"" +
                        array[7] + "\"," +
                        array[8] + "," +
                        array[9] + ",\"" +
                        array[10] + "\",\"" +
                        array[11] + "\")";
            }else if (action.equals("addMatch")) {
                query = "insert into rep_match (" +
                        "runId," +
                        "recordKey," +
                        "fileKey," +
                        "recordValue," +
                        "fileValue," +
                        "recordNumber," +
                        "fileName," +
                        "literal," +
                        "removeMultipleSpaces," +
                        "leaveOnlyNumbersAndLetters," +
                        "replaceNewLinesWithSpaces," +
                        "removeLeadingAndTrailingSpaces" +
                        ") values (" +
                        runid + ",\"" +
                        array[0] + "\",\"" +
                        array[1] + "\",\"" +
                        array[2] + "\",\"" +
                        array[3] + "\"," +
                        array[4] + ",\"" +
                        array[5] + "\",\"" +
                        array[6] + "\",\"" +
                        array[7] + "\",\"" +
                        array[8] + "\",\"" +
                        array[9] + "\",\"" +
                        array[10] + "\")";
            }else if (action.equals("addId")){
                query = "insert into rep_id (" +
                        "runId," +
                        "idname," +
                        "numFound," +
                        "idType" +
                        ") values (" +
                        runid + ",\"" +
                        array[0] + "\"," +
                        array[1] + ",\"" +
                        array[2] + "\")";
            }
            //logger.info("query: "+query);
            conn.prepareStatement(query).execute();
            conn.close();
        }catch (Exception e){
            logger.error("ERROR:",e);
        }
    }
}
