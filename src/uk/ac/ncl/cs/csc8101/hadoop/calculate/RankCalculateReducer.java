package uk.ac.ncl.cs.csc8101.hadoop.calculate;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RankCalculateReducer extends Reducer<Text, Text, Text, Text> {

    /**
     * The `reduce(...)` method is called for each <key, (Iterable of values)> pair in the grouped input.
     * Output values must be of the same type as input values and Input keys must not be altered.
     *
     * Specifically, this method should take the iterable of links to a page, along with their pagerank and number of links.
     * It should then use these to increase this page's rank by its share of the linking page's:
     *      thisPagerank +=  linkingPagerank> / count(linkingPageLinks)
     *  output:
     *      
     *      B	3.2526932	C
     *      C	3.4924011	B
     *      D	0.36398566	A,B
     *      E	0.75040764	B,D,F
     *      F	0.36398566	B,E
     *      G	0.14999998	E,B
     *      H	0.14999998	E,B
     *      I	0.14999998	E,B
     *      J	0.14999998	E
     *      K	0.14999998	E
     *
     * Note: remember pagerank's dampening factor.
     *
     * Note: Remember that the pagerank calculation MapReduce job will run multiple times, as the pagerank will get
     * more accurate with each iteration. You should preserve each page's list of links.
     *
     * @param page The individual page whose rank we are trying to capture
     * @param values The Iterable of other pages which link to this page, along with their pagerank and number of links
     * @param context The Reducer context object, to which key-value pairs are written
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {
         String[] split;
         float sumPageRanks = 0;
         String links = "";
         String content;

         for (Text value : values){                                                   
             content = value.toString();
             
             if(content.startsWith("!")){
                 links = "\t"+content.substring(1);
             }else{
	             split = content.split("\\t");
	             sumPageRanks += (Float.valueOf(split[0])/Integer.valueOf(split[1]));
             }
         }

         float newRank = 0.85F * sumPageRanks + (1-0.85F);

         context.write(page, new Text(newRank + links));
         
    }
}
