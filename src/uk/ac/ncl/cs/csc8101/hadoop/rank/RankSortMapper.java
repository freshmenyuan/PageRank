package uk.ac.ncl.cs.csc8101.hadoop.rank;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RankSortMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {

    /**
     * The `map(...)` method is executed against each item in the input split. A key-value pair is
     * mapped to another, intermediate, key-value pair.
     *
     * Specifically, this method should take Text objects in the form:
     *      `"[page]    [finalPagerank]    outLinkA,outLinkB,outLinkC..."`
     * discard the outgoing links, parse the pagerank to a float and map each page to its rank.
     *
     * Note: The output from this Mapper will be sorted by the order of its keys.
     *
     * @param key the key associated with each item output from {@link uk.ac.ncl.cs.csc8101.hadoop.calculate.RankCalculateReducer RankCalculateReducer}
     * @param value the text value "[page]  [finalPagerank]   outLinkA,outLinkB,outLinkC..."
     * @param context Mapper context object, to which key-value pairs are written
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
 
        String str=value.toString();
        String[] str1=str.split("\\t");
        FloatWritable rank = new FloatWritable(Float.valueOf(str1[1]));
        context.write(rank,new Text(str1[0]));
    }
}

//0.14999998	G	G	0.14999998	E,B
//0.14999998	H	H	0.14999998	E,B
//0.14999998	I	I	0.14999998	E,B
//0.14999998	J	J	0.14999998	E
//0.14999998	K	K	0.14999998	E
//0.36398566	D	D	0.36398566	A,B
//0.36398566	F	F	0.36398566	B,E
//0.75040764	E	E	0.75040764	B,D,F
//3.2526932		B	B	3.2526932	C
//3.4924011		C	C	3.4924011	B