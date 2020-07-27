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
        if(!SingleCountryData[5].equals("City") || !SingleCountryData[3].equals("Payment_Type"))
		    output.collect(new Text(SingleCountryData[5] + "," + SingleCountryData[3] + ","), new IntWritable( Integer.parseInt(SingleCountryData[2])));
	}
}
