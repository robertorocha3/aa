package local.domain1;

import gnu.getopt.Getopt;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PiePlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import java.io.File;
import java.sql.ResultSet;

public class Charts {
    public static void main(String[] args) {
        new Charts(args);
    }
    public Charts(){}
    public Charts(String[] args){

        Getopt g = new Getopt("Chart", args, "t:r:");
        int c,height=0;
        String arg = "",action="",runId="",mainTitleText="",title1="",title2="";
        char chartType = '1';
        boolean legend = true;
        while ((c = g.getopt()) != -1){
            switch(c){
                case 't':
                    arg = g.getOptarg();
                    if (arg.equals("1")) {
                        mainTitleText = "Top 5 Successful Extensions per Domain";
                        title1 = "Extensions";
                        title2 = "Successful downloads";
                        action = "1_multipleQueriesWithID";
                    }else if (arg.equals("2")){
                        mainTitleText = "Top 5 Successful Domains per Extension";
                        title1 = "Domains";
                        title2 = "Successful downloads";
                        action = "2_multipleQueriesWithID";
                    }else if (arg.equals("3")){
                        mainTitleText = "Run Duration";
                        title1 = "Runs";
                        title2 = "Duration in seconds";
                        action = "3_threeColumns";
                    }else if (arg.equals("4")){
                        mainTitleText = "Match Types of All Runs";
                        title1 = "Types";
                        title2 = "Number Of Occurrences";
                        action = "4_matchTypes";
                    }else if (arg.equals("5")){
                        mainTitleText = "Successful Downloads x Duration";
                        title1 = "Duration in seconds";
                        title2 = "Successful Downloads";
                        action = "5";
                        chartType = '2';
                    }else if (arg.equals("6")){
                        mainTitleText = "Analysis Duration Averages per Run";
                        title1 = "Runs";
                        title2 = "Analysis Time";
                        action = "6_singleQueryNoID";
                    }else if (arg.equals("7")){
                        mainTitleText = "File Size Averages per Domain";
                        title1 = "Domains";
                        title2 = "File Size Average (in bytes)";
                        action = "7_singleQueryWithID";
                    }else if (arg.equals("8")){
                        mainTitleText = "Successful Download Duration Averages";
                        title1 = "Runs";
                        title2 = "Duration average";
                        action = "8_singleQueryNoID";
                    }else if (arg.equals("9")){
                        mainTitleText = "10 Highest Numbers of Downloads per Domain";
                        title1 = "Domains";
                        title2 = "Number Of Downloads";
                        action = "9_singleQueryWithID";
                    }else if (arg.equals("10")){
                        mainTitleText = "Number of Downloads per Extension";
                        title1 = "Extensions";
                        title2 = "Number Of Downloads";
                        action = "10_singleQueryWithID";
                    }else if (arg.equals("11")){
                        mainTitleText = "Number of Successful Downloads per Domain";
                        title1 = "Domains";
                        title2 = "Number of Successful Downloads";
                        action = "11_singleQueryWithID";
                    }else if (arg.equals("12")){
                        mainTitleText = "Number of Successful Downloads per Extension";
                        title1 = "Extensions";
                        title2 = "Number of Successful Downloads";
                        action = "12_singleQueryWithID";
                    }else if (arg.equals("13")){
                        mainTitleText = "Download Failures per Run";
                        title1 = "Runs";
                        title2 = "Download Failures";
                        action = "13_singleQueryNoID";
                    }else if (arg.equals("14")){
                        mainTitleText = "Download Successes per Run";
                        title1 = "Runs";
                        title2 = "Download Successes";
                        action = "14_singleQueryNoID";
                    }else if (arg.equals("15")){
                        mainTitleText = "Match Fields per Run";
                        title1 = "Runs";
                        title2 = "Match Fields";
                        action = "15_singleQueryNoID";
                    }else if (arg.equals("16")){
                        mainTitleText = "20 Highest Number of Matches per Record per Run";
                        title1 = "Record Numbers";
                        title2 = "Number Of Matches Per Record";
                        action = "16_singleQueryWithID";
                    }else if (arg.equals("17")){
                        mainTitleText = "Number of Download Codes per Run";
                        title1 = "Download Codes";
                        title2 = "Number Of Occurrences";
                        action = "17_singleQueryWithID";
                    }else if (arg.equals("18")){
                        mainTitleText = "Number of Download Codes";
                        title1 = "Download Codes";
                        title2 = "Number Of Occurrences";
                        action = "18_singleQueryNoID";
                    }else if (arg.equals("19")){
                        mainTitleText = "All ID Names";
                        title1 = "ID Names";
                        title2 = "Number Of Occurrences";
                        action = "19_threeColumns";
                    }else if (arg.equals("20")){
                        mainTitleText = "Match Records";
                        title1 = "Runs";
                        title2 = "Number Of Occurrences";
                        action = "20_singleQueryNoID";
                    }else if (arg.equals("21")){
                        mainTitleText = "Match Types per Run";
                        title1 = "Types";
                        title2 = "Number Of Occurrences";
                        action = "21_matchTypes";
                    }
                    break;
                case 'r':
                    runId = g.getOptarg();
                    break;
                default:
                    System.out.println("Program syntax:\nCharts -t <1|2|3|4> -r <runId>\n");
                    System.out.println("1 = Successful extensions per domain per run.");
                    System.out.println("2 = Successful domains per extension per run.");
                    System.out.println("3 = Run duration.");
                    System.out.println("4 = Number of runs per aggregator.");
                    System.out.println("runId = Run ID number.");
                    System.out.println("Both -t and -r arguments are mandatory for types 1 and 2. -r arguments used with option 3 or 4 will be ignored.");
                    System.out.println("Example: Charts -t 1 -r 1\n");
                    break;
            }
        }
        switch (chartType){
            case '1':
                createBarChart(
                        mainTitleText, // mainTitle
                        title1,
                        title2,
                        action,
                        System.currentTimeMillis(),
                        runId,
                        height,
                        legend
                );
                break;
            case '2':
                createPlotChart(
                        mainTitleText, // mainTitle
                        title1,
                        title2,
                        action,
                        runId,
                        legend
                );
                break;
            case '3':
                createPieChart(
                        mainTitleText, // mainTitle
                        action,
                        runId,
                        legend
                );
                break;
            default:
                break;
        }
    }
    public void createBarChart(
            String mainTitle,
            String title1,
            String title2,
            String action,
            long timeID,
            String runId,
            int height,
            boolean legend
    ){
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        ResultSet rs = null;
        String query = "", subject = "",name = "",s="";
        int width = 1000,value = 0,i;
        PlotOrientation orientation = PlotOrientation.HORIZONTAL;
        try {
            java.sql.Connection conn = new Db().getConn();
            if (action.endsWith("_multipleQueriesWithID")) {

                String rs3query = "", item1 = "", item2 = "", col1 = "", col2 = "";
                orientation = PlotOrientation.VERTICAL;
                //height = 360;
                height = 400;
                if (action.startsWith("1")) {
                    col1 = "extname";
                    col2 = "domname";
                } else if (action.startsWith("2")) {
                    col1 = "domname";
                    col2 = "extname";
                }
                ResultSet rs1 = conn.prepareStatement("select distinct " + col1 + ",count(*) as count from rep_download where runid = " + runId + " and success = \"y\" group by "+col1+" order by count desc limit 5").executeQuery();
                ResultSet rs3 = null;
                while (rs1.next()) {
                    item1 = rs1.getString(1); // row key
                    System.out.println("-- "+item1);
                    rs3query = "select distinct "+col2+",count(*) as count from rep_download where "+col2+" = `"+col2+"` and "+col1+" = \""+item1+"\" and success = \"y\" and runId = "+runId+" group by "+col2+" order by count desc limit 5";
                    rs3 = conn.prepareStatement(rs3query).executeQuery();
                    while (rs3.next()) {
                        item2 = rs3.getString(1); // column key
                        value = rs3.getInt(2);
                        System.out.println(item2+": "+value);
                        dataset.addValue(
                                value,
                                item1,
                                item2
                        );
                    }
                    rs3.close();
                }
                rs1.close();
            }else if (action.endsWith("_singleQueryNoID")) {
                subject = "Run";
                if (action.startsWith("6")) {
                    query = "select distinct runid, sum(analysisduration)/(select count(*) from rep_download where runId = `runid` " +
                            "and success = \"y\") from rep_record where runid in (select distinct id from rep_run) group by runid order by runid";
                } else if (action.startsWith("13")) {
                    query = "select distinct runid, count(*) as count from rep_download where success = \"n\" and runid in (select distinct id from rep_run) group by runid order by runid";
                } else if (action.startsWith("14")) {
                    query = "select distinct runid, count(*) as count from rep_download where success = \"y\" and runid in (select distinct id from rep_run) group by runid order by runid";
                } else if (action.startsWith("15")) {
                    query = "select distinct runid, count(*) as count from rep_match where runid in (select distinct id from rep_run) group by runid order by runid";
                } else if (action.startsWith("18")) {
                    query = "select distinct statusMessage, count(*) as count from rep_download group by statusMessage order by statusMessage";
                    height = 420;
                } else if (action.startsWith("20")) {
                    query = "select distinct runid, count(distinct recordnumber) from rep_match where runid in (select distinct id from rep_run) group by runid order by runid";
                } else if (action.startsWith("8")) {
                    query = "select runid,(SUM(finishtime-starttime)/count(*)) FROM rep_download where runid in (select distinct id from rep_run) group by runid order by runid;";
                }
                rs = conn.prepareStatement(query).executeQuery();
                while (rs.next()) {
                    dataset.addValue(rs.getInt(2), subject, rs.getString(1));
                }
                runId = "";
            }else if (action.endsWith("_threeColumns")) {
                if (action.startsWith("3")) {
                    query = "select id, repname,floor((finishtime-starttime)/ 1000) from rep_run";
                } else if (action.startsWith("19")) {
                    query = "select runid,idname,numfound from rep_id";
                }
                rs = conn.prepareStatement(query).executeQuery();
                while (rs.next()) {
                    height++;
                    dataset.addValue(rs.getInt(3), rs.getString(1), rs.getString(2));
                }
                height = height*80;
                runId = "";
            }else if (action.endsWith("_singleQueryWithID")) {
                subject = "Domain";
                orientation = PlotOrientation.VERTICAL;
                if (action.startsWith("7")) {
                    query =
                            "select distinct domname," +
                                    "floor((select sum(filesize) from rep_download where runid = " + runId + " and domname = q1.domname and success = \"y\")/(select count(*) " +
                                    "from rep_download where domname = q1.domname and success = \"y\" and runid = " + runId + ")) as averages " +
                                    "from rep_download as q1 " +
                                    "where runid = " + runId + " and success = \"y\" " +
                                    "group by domname order by averages desc limit 5";
                    orientation = PlotOrientation.HORIZONTAL;
                } else if (action.startsWith("9")) {
                    query = "select distinct domname, count(*) as count from rep_download where runid = " + runId + " group by domname order by count desc limit 10";
                    orientation = PlotOrientation.HORIZONTAL;
                } else if (action.startsWith("10")) {
                    query = "select distinct extname, count(*) as count from rep_download where runid = " + runId + " group by extname order by count desc limit 20";
                    subject = "Extensions";
                } else if (action.startsWith("11")) {
                    query = "select distinct domname, count(*) as count from rep_download where success = \"y\" and runid = " + runId + " group by domname order by count desc limit 10";
                    orientation = PlotOrientation.HORIZONTAL;
                } else if (action.startsWith("12")) {
                    query = "select distinct extname, count(*) as count from rep_download where success = \"y\" and runid = " + runId + " group by extname order by count desc limit 10";
                    subject = "Extensions";
                } else if (action.startsWith("16")) {
                    query = "select distinct recordnumber, count(*) as count from rep_match where runid = " + runId + " group by recordnumber order by count desc limit 20";
                    subject = "Records";
                } else if (action.startsWith("17")) {
                    query = "select distinct statusMessage, count(*) as count from rep_download where runid=" + runId + " group by statusMessage order by count desc limit 20";
                    subject = "Download Codes";
                    height = 420;
                }
                //System.out.println(query);
                rs = conn.prepareStatement(query).executeQuery();
                while (rs.next()) {
                    i = rs.getInt(2);
                    s = rs.getString(1);
                    dataset.addValue(i, subject, s);
                }
            }else if (action.endsWith("_matchTypes")) {
                String[] values = {"literal", "removeMultipleSpaces", "leaveOnlyNumbersAndLetters", "replaceNewLinesWithSpaces", "removeLeadingAndTrailingSpaces"};
                for (int c = 0; c < values.length; c++) {
                    s = values[c];
                    if(action.startsWith("4")) { // single run
                        query = "select count(*) from rep_match where " + s + " = \"y\" and runid=" + runId;
                    }else if(action.startsWith("22")) { // all runs
                        query = "select count(*) from rep_match where " + s + " = \"y\"";
                        runId = "";
                    }
                    //System.out.println(query);
                    rs = conn.prepareStatement(query).executeQuery();
                    rs.next();
                    i = rs.getInt(1);
                    dataset.addValue(i, "Match Types", s);
                }
                runId = "";
            }
            if (!runId.equals("")){
                rs = conn.prepareStatement("select repname from rep_run where id = "+runId).executeQuery();
                if (rs.next()){
                    mainTitle = mainTitle + " - " + rs.getString(1).toUpperCase()+" - RUN "+runId;
                }
            }
            if (rs!=null){
                rs.close();
            }
            conn.close();
            JFreeChart chart = ChartFactory.createBarChart( // used to compare multiple subjects side by side
                    mainTitle,  // main title
                    title1, // horizontal title (Xaxis in a vertical orientation)
                    title2, // vertical title (Yaxis in a vertical orientation)
                    dataset,
                    orientation,
                    legend,
                    true, // tooltips
                    false // urls
            );
            CategoryPlot plot = chart.getCategoryPlot();
            CategoryAxis axis = plot.getDomainAxis();

            axis.setCategoryMargin(0.1);
            axis.setLowerMargin(0.005);
            axis.setUpperMargin(0.005);

            if (orientation == PlotOrientation.VERTICAL) {
                axis.setCategoryLabelPositions(CategoryLabelPositions.DOWN_45);
                axis.setUpperMargin(0.07);
            }

            BarRenderer renderer = (BarRenderer) plot.getRenderer();
            renderer.setItemMargin(0); // 0.1 means 10%
            renderer.setMinimumBarLength(0.6);
            /*
            for (int series = 0 ;series<dataset.getColumnCount();series++) {
                renderer.setSeriesItemLabelGenerator(series, new StandardCategoryItemLabelGenerator("{2}", NumberFormat.getNumberInstance()));
                //renderer.setSeriesPositiveItemLabelPosition(series, new ItemLabelPosition(ItemLabelAnchor.OUTSIDE12, TextAnchor.TOP_LEFT));
                renderer.setSeriesItemLabelsVisible(series, true);
            }
            */
            if (height == 0) {
                if (dataset.getColumnCount() >= dataset.getRowCount()) {
                    height = (dataset.getRowCount() + 2) * 100;
                } else if (dataset.getRowCount() > dataset.getColumnCount()) {
                    height = (dataset.getColumnCount() + 2) * 100;
                }
            }

            //System.out.println("width: "+width);
            //System.out.println("height: "+height);
            ChartUtilities.saveChartAsPNG(new File("files/png/"+mainTitle.toUpperCase()+"_"+timeID+".png"),chart,width,height);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createPlotChart(
            String mainTitle,
            String title1,
            String title2,
            String action,
            String runId,
            boolean legend
    ){
        try {
            XYSeriesCollection dataset = new XYSeriesCollection( );
            //TimeSeriesCollection dataset = new TimeSeriesCollection();
            XYSeries s;
            //TimeSeries s;
            //java.util.Date time=new java.util.Date((long)timeStamp*1000);
            java.sql.Connection conn = new Db().getConn();
            if (action.equals("5")) { // successful downloads x duration
                String name;
                double duration;
                //Date duration;
                ResultSet names = conn.prepareStatement("select distinct repname from rep_run").executeQuery();
                ResultSet runIDsAndDurationFromName = null;
                ResultSet sucDownFromID = null;
                while (names.next()) { // cycle through the names...
                    name = names.getString(1);
                    s = new XYSeries(name);
                    //s = new TimeSeries(name,Minute.class);
                    runIDsAndDurationFromName = conn.prepareStatement("select id,floor((finishtime-starttime)/ 1000) from rep_run where repname = \""+name+"\"").executeQuery();
                    //runIDsAndDurationFromName = conn.prepareStatement("select id,(finishtime-starttime) from rep_run where repname = \""+name+"\"").executeQuery();
                    //runIDsAndDurationFromName = conn.prepareStatement("select id,finishtime from rep_run where repname = \""+name+"\"").executeQuery();
                    while (runIDsAndDurationFromName.next()) {
                        duration = runIDsAndDurationFromName.getDouble(2);
                        //duration = new java.util.Date(runIDsAndDurationFromName.getLong(2));
                        sucDownFromID = conn.prepareStatement("select count(*) from rep_download where success = \"y\" and runid="+runIDsAndDurationFromName.getString(1)).executeQuery();
                        sucDownFromID.next();
                        s.add(duration,sucDownFromID.getDouble(1));
                        //s.add(new Minute(duration),sucDownFromID.getDouble(1));
                    }
                    dataset.addSeries(s);
                }
                runId = "";
            }
            conn.close();
            JFreeChart chart = ChartFactory.createXYLineChart(
            //JFreeChart chart = ChartFactory.createTimeSeriesChart(
            //JFreeChart chart = ChartFactory.createScatterPlot(
                    mainTitle,  // main title
                    title1, // horizontal title (Xaxis in a vertical orientation)
                    title2, // vertical title (Yaxis in a vertical orientation)
                    dataset,
                    PlotOrientation.VERTICAL,
                    legend,
                    true, // tooltips
                    false // urls
            );
            /*
            XYPlot plot = chart.getXYPlot();
            for (int i = 0;i < dataset.getItemCount(0) - 1;i++){
                double x1 = dataset.getSeries(0).getDataItem(i).getXValue();
                double x2 = dataset.getSeries(0).getDataItem(i + 1).getXValue();
                double y1 = dataset.getSeries(0).getDataItem(i).getYValue();
                double y2 = dataset.getSeries(0).getDataItem(i + 1).getYValue();
                double angle = Math.atan2(y1 - y2, x2 - x1) + Math.PI;
                XYPointerAnnotation arrow = new XYPointerAnnotation("",x1,y1,angle);
                if (i == 0){
                    arrow.setText("Start");
                }else if (i % 5 == 0){
                    arrow.setText(Integer.toString(i));
                }
                arrow.setLabelOffset(15.0);
                arrow.setToolTipText(Integer.toString(i));
                plot.addAnnotation(arrow);
            }
            */
            //XYPlot plot = chart.getXYPlot();
            //DateAxis axis = (DateAxis) plot.getDomainAxis();
            //axis.setDateFormatOverride(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss"));
            //axis.setDateFormatOverride(new SimpleDateFormat("HH:mm:ss"));
            int width = 640; /* Width of the image */
            int height = 480; /* Height of the image */
            ChartUtilities.saveChartAsPNG(new File("files/png/"+mainTitle.toUpperCase()+runId+"_"+System.currentTimeMillis()+".png"),chart,width,height);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void createPieChart(
            String mainTitle,
            String action,
            String runId,
            boolean legend
    ){
        try {
            ResultSet rs = null;
            String query = "";
            int height = 0;
            DefaultPieDataset dataset = new DefaultPieDataset();
            java.sql.Connection conn = new Db().getConn();
            String name = "";
            double value = 0.0;
            if (action.endsWith("_singleQueryNoID")) {
                if (action.startsWith("4")) {
                    query = "select distinct repname, count(*) as count from rep_run group by repname order by repname";
                }
                rs = conn.prepareStatement(query).executeQuery();
                while (rs.next()) {
                    name = rs.getString(1);
                    value = rs.getDouble(2);
                    dataset.setValue(name+" ("+String.valueOf(rs.getInt(2))+")",value);
                }
                runId = "";
            }
            if (rs!=null){
                rs.close();
            }
            conn.close();
            JFreeChart chart = ChartFactory.createPieChart( // used to show the progress of a single subject across time
                    mainTitle,  // main title
                    dataset,
                    legend,
                    true, // tooltips
                    false // urls
            );
            final PiePlot plot = (PiePlot) chart.getPlot( );
            plot.setStartAngle( 270 );
            plot.setForegroundAlpha( 0.60f );
            plot.setInteriorGap( 0.02 );
            int width = 640;
            height = 480;
            ChartUtilities.saveChartAsPNG(new File("files/png/"+mainTitle.toUpperCase()+runId+"_"+System.currentTimeMillis()+".png"),chart,width,height);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
