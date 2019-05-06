package com.spark.helper;
import org.apache.spark.sql.SparkSession;
import org.jfree.chart.JFreeChart;
import org.jfree.data.category.DefaultCategoryDataset;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class Driver {

	public static void main(String[] args) throws Exception {
		

		// configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("Read JSON File to DataSet")
                .master("local")
                .getOrCreate();
        
        Dataset<Row> tweet = spark.read().json("twitter_data.json");
        tweet.printSchema();
        tweet.show(100);
        System.out.println("Count : " + tweet.count());
       
        tweet.createOrReplaceTempView("tweet");
        List<Row> arrayList= new ArrayList<Row>();
        HashMap<String, Long> hashMap = new HashMap<String, Long>();
        String query = "";
        Dataset<Row> result;
        DefaultCategoryDataset dataset;
        JFreeChart barChart;
        
        String filename = "log.txt";
        String querySeprator = "*****************************************************************************************";
        FileWriter log = new FileWriter(filename);
       
        
        log.write(querySeprator);
        log.write("\n" + "Query for Languages");
        
		query = "SELECT lang, count(lang) as lang_count from tweet group by lang order by lang_count desc limit 5"; 
		result = spark.sql(query);
		result.show();
		log.write("\n" + query + "\n");
		log.write(result.showString(10, 0, false));
		arrayList = result.collectAsList();
		for(int i=0; i<arrayList.size(); i++) {
			hashMap.put(arrayList.get(i).getString(0), arrayList.get(i).getLong(1)); 
		}
		dataset = Chart.generateBarChart(hashMap, "Tweets");
        barChart = Chart.getJFreeBarChart(dataset, "Tweets By Languages","Languages", "Number of Tweets");
        Chart.printBarChartToJPEG(barChart, "Languages");
        
		log.write("\n" + querySeprator);
		log.write("\n" + "Query for Countries");
        
		query = "SELECT place.country, count(place.country) as country_count from tweet group by place.country order by country_count desc limit 5"; 
		result = spark.sql(query); 
		result.show();
		log.write("\n" + query + "\n");
		log.write(result.showString(10, 0, false));
		hashMap = new HashMap<String,Long>(); 
		arrayList = result.collectAsList(); 
		for(int i=0; i<arrayList.size(); i++) { 
			hashMap.put(arrayList.get(i).getString(0), arrayList.get(i).getLong(1)); 
		}
		Chart.generateBarChart(hashMap, "Country");
		dataset = Chart.generateBarChart(hashMap, "Tweets");
        barChart = Chart.getJFreeBarChart(dataset, "Tweets By Country","Country", "Number of Tweets");
        Chart.printBarChartToJPEG(barChart, "Country");
        
		
        log.write("\n" + querySeprator);
        log.write("\n" + "Queries for Trending Hashtags");
        
		Dataset<Row> flattened = tweet.select(org.apache.spark.sql.functions.explode(tweet.col("entities.hashtags")).as("hashtags_flat"));
        flattened.show(100);  
        flattened.printSchema();
        log.write("\n" + "tweet.select(org.apache.spark.sql.functions.explode(tweet.col(\"entities.hashtags\")).as(\"hashtags_flat\"))" + "\n");
		log.write(flattened.showString(10, 0, false));
        
        Dataset<Row> hashtags = flattened.select("hashtags_flat.text");
        hashtags.show(100); 
        log.write("\n" + "flattened.select(\"hashtags_flat.text\")" + "\n");
		log.write(hashtags.showString(10, 0, false));
        hashtags.printSchema();
        hashtags.createOrReplaceTempView("hashtags");
        
        query = "SELECT text, count(text) as hashtags_count from hashtags group by text order by hashtags_count desc limit 10";
        result = spark.sql(query);
        result.show();
        log.write("\n" + query + "\n");
		log.write(result.showString(10, 0, false));
        arrayList = result.collectAsList();
        hashMap = new HashMap<String,Long>();
        for(int i=0; i<arrayList.size(); i++) {
   		  hashMap.put(arrayList.get(i).getString(0), arrayList.get(i).getLong(1)); 
   		}
   		Chart.generatePieChart(hashMap, "Trending Hashtags");
		
		
   		log.write("\n" + querySeprator);
        log.write("\n" + "Queries for Trending Hashtag for Country");
   		
   		
        flattened = tweet.select(tweet.col("place.country").as("country"), org.apache.spark.sql.functions.explode(tweet.col("entities.hashtags")).as("hashtags_flat"));
        flattened.show(100);  
        log.write("\n" + "tweet.select(tweet.col(\"place.country\").as(\"country\"), org.apache.spark.sql.functions.explode(tweet.col(\"entities.hashtags\")).as(\"hashtags_flat\"))" + "\n");
		log.write(flattened.showString(10, 0, false));
        flattened.printSchema();
        
        hashtags = flattened.select("country", "hashtags_flat.text").filter(flattened.col("country").isNotNull());
        hashtags.show(100);  
        log.write("\n" + "flattened.select(\"country\", \"hashtags_flat.text\").filter(flattened.col(\"country\").isNotNull())" + "\n");
		log.write(hashtags.showString(10, 0, false));
        hashtags.printSchema();
        hashtags.createOrReplaceTempView("hashtags");
        
        query = "SELECT country, text, count(text) as hashtags_count from hashtags group by country, text";
        Dataset<Row> hashtagsCount = spark.sql(query);
        hashtagsCount.show(100);
        log.write("\n" + query + "\n");
		log.write(hashtagsCount.showString(10, 0, false));
        hashtagsCount.printSchema();
        hashtagsCount.createOrReplaceTempView("hashtagscount");
       
        query = "WITH cte as (select *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY hashtags_count DESC) AS rn FROM hashtagscount) select * from cte where rn=1";
        result = spark.sql(query);
        result.show(100);
        log.write("\n" + query + "\n");
		log.write(result.showString(10, 0, false));
        arrayList = result.collectAsList();
   		Chart.generateBarChartWithValues(arrayList, "Trending Hashtag for Country");
   		
   		
   		log.write("\n" + querySeprator);
        log.write("\n" + "Queries for Verified Accounts");
   		
   		
   		query = "SELECT count(*) from tweet where user.verified = true";
        result = spark.sql(query);
        result.show(100);
        log.write("\n" + query + "\n");
		log.write(result.showString(10, 0, false));
        arrayList = result.collectAsList();
        hashMap = new HashMap<String,Long>();
        for(int i=0; i<arrayList.size(); i++) {
        	hashMap.put("Verfied Accounts", arrayList.get(i).getLong(0));
        }
        query = "SELECT count(*) from tweet where user.verified = false";
        result = spark.sql(query);
        result.show(100);
        log.write("\n" + query + "\n");
		log.write(result.showString(10, 0, false));
        arrayList = result.collectAsList();
        for(int i=0; i<arrayList.size(); i++) {
        	hashMap.put("Unverfied Accounts", arrayList.get(i).getLong(0));
        }
        Chart.generatePieChart(hashMap, "Verified Accounts");
        
        
        log.write("\n" + querySeprator);
        log.write("\n" + "Queries for Verified Accounts By Country");
        
        
        result = tweet.select("place.country", "user.verified").filter(tweet.col("place.country").isNotNull());
        result.show(100);
        log.write("\n" + "tweet.select(\"place.country\", \"user.verified\").filter(tweet.col(\"place.country\").isNotNull())" + "\n");
		log.write(result.showString(10, 0, false));
        result.createOrReplaceTempView("verified_accounts");
        
        query = "select country, count(verified) as verified_count from verified_accounts where verified = true group by country";
        result = spark.sql(query);
        result.show(100);
        log.write("\n" + query + "\n");
		log.write(result.showString(10, 0, false));
        arrayList = result.collectAsList();
        hashMap = new HashMap<String,Long>();
        for(int i=0; i<arrayList.size(); i++) {
        	hashMap.put(arrayList.get(i).getString(0), arrayList.get(i).getLong(1));
        }
        Chart.generatePieChart(hashMap, "Verified Accounts By Country");
        
        
        log.write("\n" + querySeprator);
        log.write("\n" + "Query for Average Followers count of Verified Accounts By Languages");
        
        
        query = "SELECT lang, avg(user.followers_count) as avg_followers_count from tweet where user.verified = true group by user.verified, lang order by avg_followers_count desc";
        result = spark.sql(query);
        result.show(100);
        log.write("\n" + query + "\n");
		log.write(result.showString(10, 0, false));
        arrayList = result.collectAsList();
        hashMap = new HashMap<String,Long>();
        for(int i=0; i<arrayList.size(); i++) {
        	long avg = (long) arrayList.get(i).getDouble(1);
        	hashMap.put(arrayList.get(i).getString(0), avg);
        }
        dataset = Chart.generateBarChart(hashMap, "Average Followers count of Verified Accounts By Languages");
        barChart = Chart.getJFreeBarChart(dataset, "Average Followers count of Verified Accounts By Languages","Languages", "Avg Followers Count");
        Chart.printBarChartToJPEG(barChart, "Average Followers count of Verified Accounts By Languages");
        
        
        log.write("\n" + querySeprator);
        log.write("\n" + "Queries For Top 10 User Mentions");
        
        
        flattened = tweet.select(org.apache.spark.sql.functions.explode(tweet.col("entities.user_mentions")).as("user_mentions_flat"));
        flattened.show(100); 
        log.write("\n" + "tweet.select(org.apache.spark.sql.functions.explode(tweet.col(\"entities.user_mentions\")).as(\"user_mentions_flat\"))" + "\n");
		log.write(flattened.showString(10, 0, false));
        flattened.printSchema();
        
        Dataset<Row> usermention = flattened.select("user_mentions_flat.name");
        usermention.show(100); 
        log.write("\n" + "flattened.select(\"user_mentions_flat.name\")" + "\n");
		log.write(usermention.showString(10, 0, false));
        usermention.printSchema();
        usermention.createOrReplaceTempView("usermention");
        
        query = "select name, count(name) as name_count from usermention group by name order by name_count desc limit 10";
        result = spark.sql(query);
        result.show(100);
        log.write("\n" + query + "\n");
		log.write(result.showString(10, 0, false));
        arrayList = result.collectAsList();
        hashMap = new HashMap<String,Long>();
        for(int i=0; i<arrayList.size(); i++) {
        	hashMap.put(arrayList.get(i).getString(0), arrayList.get(i).getLong(1));
        }
        dataset = Chart.generateBarChart(hashMap, "Number of User Mentions");
        barChart = Chart.getJFreeBarChart(dataset, "Top 10 User Mentions","User Mentions", "Number of User Mentions");
        Chart.printBarChartToJPEG(barChart, "Top 10 User Mentions");
        
        
        log.write("\n" + querySeprator);
        log.write("\n" + "Queries for Most number of Language used in each Country");
        
        
        result = tweet.select("place.country", "lang").filter(tweet.col("place.country").isNotNull());
		result.show(100);
		log.write("\n" + "tweet.select(\"place.country\", \"lang\").filter(tweet.col(\"place.country\").isNotNull())" + "\n");
		log.write(result.showString(10, 0, false));
		result.printSchema();
		result.createOrReplaceTempView("country_lang");
		
		query = "SELECT country, lang, count(lang) as lang_count from country_lang group by country, lang";
		Dataset<Row> countrylangcount = spark.sql(query);
		countrylangcount.show(100);
		log.write("\n" + query + "\n");
		log.write(countrylangcount.showString(10, 0, false));
		countrylangcount.createOrReplaceTempView("country_lang_count");
       	
		query = "WITH cte as (select *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY lang_count DESC) AS rn FROM country_lang_count) select * from cte where rn=1";
        result = spark.sql(query);
        result.show(100);
        log.write("\n" + query + "\n");
		log.write(result.showString(10, 0, false));
        arrayList = result.collectAsList();
   		Chart.generateBarChartWithValues(arrayList, "Most number of Language used in each Country");
       
   		
   		log.write("\n" + querySeprator);
   		log.write("\n" + "Queries for Number of Users per year");

   		
        query = "SELECT year(cast(to_timestamp(user.created_at, 'EEE MMM dd HH:mm:ss Z yyyy')as date)) as year, count(user.id)as user_count FROM tweet where group by year(cast(to_timestamp(user.created_at, 'EEE MMM dd HH:mm:ss Z yyyy')as date)) order by year(cast(to_timestamp(user.created_at, 'EEE MMM dd HH:mm:ss Z yyyy')as date))";
        result = spark.sql(query);
        result.show(100);
        log.write("\n" + query + "\n");
		log.write(result.showString(10, 0, false));
		result.printSchema();
		result = result.select("year","user_count").filter(result.col("year").isNotNull());
		result.show(100);
		log.write("\n" + "result.select(\"year\",\"user_count\").filter(result.col(\"year\").isNotNull())" + "\n");
		log.write(result.showString(10, 0, false));
		arrayList = result.collectAsList();
        LinkedHashMap<Integer, Long> map = new LinkedHashMap<Integer,Long>();
        for(int i=0; i<arrayList.size(); i++) {
        	map.put(arrayList.get(i).getInt(0), arrayList.get(i).getLong(1));
        }
        Chart.printLineChart(map, "Number of Users per year");

        log.close();
        spark.stop();
	}
	
}
