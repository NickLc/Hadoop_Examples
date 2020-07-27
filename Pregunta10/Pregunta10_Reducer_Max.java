package SalesCountry;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        
    int count = 0;
    private int maxValue = 0;
    private Text outKey = new Text();
    OutputCollector<Text, IntWritable> output;

	public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
        
        this.output = output;	
        Text key = t_key;
        int frequencyCountry = 0;
        
		while (values.hasNext()) {
        
            IntWritable value = (IntWritable) values.next();
            frequencyCountry += value.get();
            this.count++;
        
        }
                
        if(frequencyCountry > maxValue){
            maxValue = frequencyCountry;
            this.outKey.set(key);
        }
        if(this.count == 998)
            output.collect(this.outKey, new IntWritable(maxValue));
	}
}