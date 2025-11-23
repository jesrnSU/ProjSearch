package engine.search;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.LinkedBlockingDeque;

/*
    Should be reworked to use BufferedStream instead of RandomAccessFile
    since it's more efficient and supports characters like åäö
*/

public class FileChunkReader implements Runnable{
    private File textFile;
    private long startOffset;
    private long endOffset;
    private LinkedBlockingDeque<CoursePage> output; 

    /*
        Simulation of a mapper. Each mapper object reads lines, retrieves the key (URL) and the value (Text) from the line.  
        Then sends this information as a CoursePage into a LinkedBlockingDeque that can be fetched form. 
    */

    public FileChunkReader(File textFile, long startOffset, long endOffset, LinkedBlockingDeque<CoursePage> output){
        this.textFile = textFile;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.output = output;
    }

    @Override
    public void run(){
        try(FileInputStream fileInputStream = new FileInputStream(textFile)){
            FileChannel fChannel = fileInputStream.getChannel();
            fChannel.position(startOffset);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream, StandardCharsets.UTF_8));
            long currentBytesPos = startOffset;

            // StartOffset might be in the middle of a line, make sure reading begins at a newline 
            if(startOffset > 0){
                String skipLine = reader.readLine();
                if(skipLine != null){
                    currentBytesPos += skipLine.getBytes(StandardCharsets.UTF_8).length + 1;            
                }
            }

            String textLine; 
            int tabIndex;

            while((textLine = reader.readLine()) != null){
                int lineSize = textLine.getBytes(StandardCharsets.UTF_8).length + 1;

                if(currentBytesPos > endOffset){
                    break;
                }

                tabIndex = textLine.indexOf('\t');
                String url = textLine.substring(0, tabIndex);
                String pageContent = textLine.substring(tabIndex + 1); 
                output.put(new CoursePage(url, pageContent));

                currentBytesPos += lineSize;
            }
        }catch(IOException | InterruptedException e){
            e.printStackTrace();
            Thread.currentThread().interrupt();
        } 
    }
}
