package engine.search;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Reads a specific chunk of a file containing crawled data.
 * <p>
 * This class implements the {@link Runnable} interface to allow parallel processing of file chunks.
 * It acts as a <strong>Mapper</strong> in the MapReduce simulation. It reads lines from the file,
 * parses them into {@link CoursePage} objects (Key: URL, Value: Content), and places them into a
 * shared {@link LinkedBlockingDeque} for further processing by Sorters.
 * </p>
 */
public class FileChunkReader implements Runnable {
    private File textFile;
    private long startOffset;
    private long endOffset;
    private LinkedBlockingDeque<CoursePage> output; 

    /**
     * Constructs a FileChunkReader to read a specific segment of a file.
     *
     * @param textFile      The source file to read from.
     * @param startOffset   The byte offset where reading should begin.
     *                      If this offset falls in the middle of a line, the reader will skip
     *                      to the beginning of the next line to ensure data integrity.
     * @param endOffset     The byte offset where reading should stop. The reader will continue
     *                      reading until the current position exceeds this offset.
     * @param output        The thread-safe queue where parsed {@link CoursePage} objects are put.
     *                      This acts as the output channel for the Mapper.
     */
    public FileChunkReader(File textFile, long startOffset, long endOffset, LinkedBlockingDeque<CoursePage> output){
        this.textFile = textFile;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.output = output;
    }

    /**
     * Executes the file reading task.
     * <p>
     * Opens the file, seeks to the specified {@code startOffset}, and reads line by line.
     * It handles the boundary condition where a chunk starts in the middle of a line by
     * skipping the partial line (assuming the previous chunk covered it).
     * </p>
     */
    @Override
    public void run(){
        try(FileInputStream fileInputStream = new FileInputStream(textFile)){
            FileChannel fChannel = fileInputStream.getChannel();
            fChannel.position(startOffset);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream, StandardCharsets.UTF_8));
            long currentBytesPos = startOffset;

            // Boundary Handling:
            // If startOffset is not 0, we might have landed in the middle of a line.
            // We skip the rest of this partial line because the previous mapper (which ended at startOffset)
            // is responsible for reading until the newline character.
            if(startOffset > 0){
                String skipLine = reader.readLine();
                if(skipLine != null){
                    // +1 accounts for the newline character (approximate for LF, might vary for CRLF)
                    currentBytesPos += skipLine.getBytes(StandardCharsets.UTF_8).length + 1;            
                }
            }

            readFileLineByLine(reader, currentBytesPos);

       }catch(IOException | InterruptedException e){
            e.printStackTrace();
            // Restore interrupt status if interrupted
            Thread.currentThread().interrupt();
        } 
    }

    /**
     * Reads lines from the BufferedReader and processes them until the end offset is reached.
     *
     * @param reader          The buffered reader initialized at the correct position.
     * @param currentBytesPos The current byte position tracker.
     * @throws IOException          If an I/O error occurs.
     * @throws InterruptedException If the thread is interrupted while waiting to put into the queue.
     */
    private void readFileLineByLine(BufferedReader reader, long currentBytesPos) throws IOException, InterruptedException{
        String textLine; 
        int tabIndex;

        while((textLine = reader.readLine()) != null){
            // Calculate the size of the line in bytes to track progress
            int lineSize = textLine.getBytes(StandardCharsets.UTF_8).length + 1;

            // Stop Condition:
            // If we have read past our assigned chunk (endOffset), we stop.
            // Note: We check AFTER reading a line to ensure we don't stop in the middle of a line.
            if(currentBytesPos > endOffset){
                break;
            }

            // Parsing Logic:
            // Expects format: "URL\tContent"
            tabIndex = textLine.indexOf('\t');
            if (tabIndex != -1) {
                String url = textLine.substring(0, tabIndex);
                String pageContent = textLine.substring(tabIndex + 1); 
                
                // Producer action: Put into blocking queue (waits if queue is full)
                output.put(new CoursePage(url, pageContent));
            }

            currentBytesPos += lineSize;
        }
    }
}
