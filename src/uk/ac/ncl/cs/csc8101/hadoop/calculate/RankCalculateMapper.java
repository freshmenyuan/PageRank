package uk.ac.ncl.cs.csc8101.hadoop.calculate;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RankCalculateMapper extends Mapper<LongWritable, Text, Text, Text> {
    /**
     * The `map(...)` method is executed against each item in the input split. A key-value pair is
     * mapped to another, intermediate, key-value pair.
     *
     * Specifically, this method should take Text objects in the form
     *      `"[page]    [initialPagerank]    outLinkA,outLinkB,outLinkC..."`
     * and store a new key-value pair mapping linked pages to this page's name, rank and total number of links:
     *      `"[otherPage]   [thisPage]    [thisPagesRank]    [thisTotalNumberOfLinks]"
     *
     * Note: Remember that the pagerank calculation MapReduce job will run multiple times, as the pagerank will get
     * more accurate with each iteration. You should preserve each page's list of links.
     *
     * @param key the key associated with each item output from {@link uk.ac.ncl.cs.csc8101.hadoop.parse.PageLinksParseReducer PageLinksParseReducer}
     * @param value the text value "[page]  [initialPagerank]   outLinkA,outLinkB,outLinkC..."
     * @param context Mapper context object, to which key-value pairs are written
     * @throws IOException
     * @throws InterruptedException
     * 
     * //Reducer stores pages and outgoing links in the format: "[page]  [initialPagerank]   outLinkA,outLinkB,outLinkC..."
     * input sample:
     * 
     * key  value
     * ---------------------------------------
	 * B	1.0	C
	 * C	1.0	B
	 * D	1.0	A,B
	 * E	1.0	B,D,F
	 * F	1.0	B,E
	 * G	1.0	E,B
	 * H	1.0	E,B
	 * I	1.0	E,B
	 * J	1.0	E
	 * K	1.0	E
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
    	String str=value.toString();
    	String[] split=str.split("\\t");
    	String links="";
    	String[] linkPages = null;
    	int NumberLinks=0;
    	String mianPage=split[0];
    	String rank=split[1];
    	
        if(split.length==3){                          // check if it has links
        	links = split[2];
            linkPages = links.split(",");
            NumberLinks = linkPages.length;
            
            for (String page : linkPages){
                context.write(new Text(page), new Text(rank +"\t"+ NumberLinks));  // Format page rank number
            }
            context.write(new Text(mianPage), new Text("!" + links));    // Format pages and links 
            
        }else{                                        // if no links then do not put to reduce 
        	context.write(new Text(mianPage), new Text("!"));
        }
  
    }
}

//output

//A	1.0	2
//B	!C
//B	1.0	2
//B	1.0	2
//B	1.0	2
//B	1.0	2
//B	1.0	1
//B	1.0	3
//B	1.0	2
//C	1.0	1
//C	!B
//D	1.0	3
//D	!A,B
//E	1.0	2
//E	!B,D,F
//E	1.0	2
//E	1.0	2
//E	1.0	1
//E	1.0	2
//E	1.0	1
//F	!B,E
//F	1.0	3
//G	!E,B
//H	!E,B
//I	!E,B
//J	!E
//K	!E