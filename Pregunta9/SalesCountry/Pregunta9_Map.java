package SalesCountry;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {

		String valueString = value.toString();
		String[] SingleCountryData = valueString.split(",");

		if(!SingleCountryData[4].equals("Name") || !SingleCountryData[7].equals("Country"))
			output.collect(new Text(SingleCountryData[4] + "," + SingleCountryData[7] + ","), new IntWritable(Integer.parseInt(SingleCountryData[2])) ) ;
	}
}