package Dictionary;
import java.io.IOException;
import java.util.StringTokenizer;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class CacheMapper  extends Mapper<LongWritable, Text, Text, Text> {
    String fileName=null, language=null;
	   public Map<String, String> translations = new HashMap<String, String>();
	   
	   BufferedReader brReader ;
    
	   public void setup(Context context) throws IOException, InterruptedException{
           // TODO: determine the name of the additional language based on the file name
           // TODO: OPTIONAL: depends on your implementation -- create a HashMap of translations (word, part of speech, translations) from output of exercise 1
           Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
           for (Path eachPath : cacheFilesLocal) {
               if (eachPath.getName().toString().trim().equals("latin.txt")) {
                   fileName= eachPath.getName();
                   additionalLanguageHashMap(eachPath, context);
               }
           }
       }
    
    private void additionalLanguageHashMap(Path filePath, Context context)
    throws IOException
    {
        String strLineRead = "";
        
        try {
            brReader = new BufferedReader(new FileReader(filePath.toString()));
            
            // Read each line, split and load to HashMap
            while ((strLineRead = brReader.readLine()) != null) {
                
                if(strLineRead.contains("[Noun]")||strLineRead.contains("[Verb]")||strLineRead.contains("[Conjunction]")||strLineRead.contains("[Adverb]")||strLineRead.contains("[Adjective]")||strLineRead.contains("[Pronoun]")||strLineRead.contains("[Preposition]")||strLineRead.contains("[interjection]"))
                    
                {
                    
                    StringTokenizer itr = new StringTokenizer(strLineRead, "\t");
                    String valueLatin=null;
                    String keyLatin=null;
                    
                    while (itr.hasMoreTokens()) {
                        
                        keyLatin = itr.nextToken();
                        valueLatin = itr.nextToken();
                    }
                    
                    String partsOfSpeech = valueLatin.toString().substring(valueLatin.toString().lastIndexOf('[')+1,valueLatin.toString().length()-1);
                    String  keyStr = keyLatin + ":" + partsOfSpeech ;
                    String  wordStr = fileName + ":" + translations ;
                    String translationValue = valueLatin.toString().substring(0,valueLatin.toString().lastIndexOf('[') );
                    String translationLatin = "latin" + ":" + translations;
                    
                    translations.put(keyStr.trim(), translationLatin.trim());
                }
                
            }
            
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
            
        } finally
        {
            if (brReader != null) {
                brReader.close();
                
            }
            
        }
        
    }
    
	   public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
           // TODO: perform a map-side join between the word/part-of-speech from exercise 1 and the word/part-of-speech from the distributed cache file
           
           // TODO: where there is a match from above, add language:translation to the list of translations in the existing record (if no match, add language:N/A
           String valueLatin;
           if(translations.containsKey(key))
           {
               
               valueLatin = value.toString() + "|" + translations.get(key) ;
           }
           
           else {
               valueLatin =    value.toString()  + "|" + "latin: NA"   ;
               
           }
           
           context.write(key, new Text(valueLatin));	
           
	      }
}
