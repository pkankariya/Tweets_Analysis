package com.spark.helper;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.spark.sql.Row;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.chart.ChartUtilities;

public class Chart {
   
   public static DefaultCategoryDataset generateBarChart(HashMap<String, Long> languages, String rowKey)throws Exception {
	   final DefaultCategoryDataset dataset = new DefaultCategoryDataset();
	   for (String key : languages.keySet()){
		   dataset.addValue(languages.get(key), rowKey, key);
	   }
	   
	   return dataset;
   }
   
   public static JFreeChart getJFreeBarChart(DefaultCategoryDataset dataset, String s, String x, String y) {
	   JFreeChart barChart = ChartFactory.createBarChart(
		         s, 
		         x, y, 
		         dataset,PlotOrientation.VERTICAL, 
		         true, true, false);
	   return barChart;
   }
    
   public static void printBarChartToJPEG(JFreeChart barChart, String chartName) throws IOException {
	   int width = 2000;    /* Width of the image */
	   int height = 2000;   /* Height of the image */ 
	   File BarChart = new File(chartName +".jpeg"); 
	   ChartUtilities.saveChartAsJPEG(BarChart, barChart, width, height);
   }
   
   public static void generateBarChartWithValues(List<Row> arrayList, String chartName)throws Exception {
	   final DefaultCategoryDataset dataset = new DefaultCategoryDataset();
	   for(int i=0; i<arrayList.size(); i++) {
		   dataset.addValue(arrayList.get(i).getLong(2), arrayList.get(i).getString(1), arrayList.get(i).getString(0));
	   }
	   
	   JFreeChart barChart = ChartFactory.createBarChart(
         chartName, 
         chartName, "Number of Hashtags", 
         dataset,PlotOrientation.VERTICAL, 
         true, true, false);
         
	   int width = 2000;    /* Width of the image */
	   int height = 2000;   /* Height of the image */ 
	   File BarChart = new File(chartName +".jpeg"); 
	   ChartUtilities.saveChartAsJPEG(BarChart, barChart, width, height );
   }
   
   public static void generatePieChart(HashMap<String, Long> hashmap, String chartName) throws IOException {
		DefaultPieDataset dataSet = new DefaultPieDataset();
		for (String key : hashmap.keySet()){
			dataSet.setValue(key, hashmap.get(key));
		}
		
		JFreeChart pieChart = ChartFactory.createPieChart(
				"", dataSet, true, true, false);

		int width = 640;    /* Width of the image */
		int height = 480;   /* Height of the image */ 
		File PieChart = new File(chartName +".jpeg"); 
		ChartUtilities.saveChartAsJPEG(PieChart, pieChart, width, height);
	}
   	
   	public static void printLineChart(LinkedHashMap<Integer, Long> hashmap, String chartName) throws Exception {
	
   		DefaultCategoryDataset line_chart_dataset = new DefaultCategoryDataset();
   		for (Integer key : hashmap.keySet()){
   			line_chart_dataset.addValue(hashmap.get(key), "Users", key);
   		}
	    JFreeChart lineChartObject = ChartFactory.createLineChart(
	         "Users Vs Years","Year",
	         "Users Count",
	         line_chart_dataset,PlotOrientation.VERTICAL,
	         true,true,false);

	    int width = 2000;    /* Width of the image */
	    int height = 2000;   /* Height of the image */ 
	    File lineChart = new File(chartName +".jpeg" ); 
	    ChartUtilities.saveChartAsJPEG(lineChart ,lineChartObject, width ,height);
   		
   	}  
}