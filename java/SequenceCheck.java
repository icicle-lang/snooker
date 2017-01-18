import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceCheck {

    private Configuration conf = new Configuration();
    /*private FileSystem fs;
    {
        try {
            fs = FileSystem.get(URI.create("hdfs://cldx-1336-1202:9000"), conf);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }*/

    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub

        if (args == null || args.length < 2) {

            System.out
                    .println("Following are the possible invocations <operation id> <arg1> <arg2> ...");

            System.out
                    .println("1 <absolute path of directory containing documents> <HDFS path of the sequence file");

            System.out.println("2 <HDFS path of the sequence file>");
            return;
        }

        int operation = Integer.valueOf(args[0]);

        SequenceCheck docToSeqFileWriter = new SequenceCheck();

        switch (operation) {

        case 1: {
            String docDirectoryPath = args[1];
            String sequenceFilePath = args[2];

            System.out.println("Writing files present at " + docDirectoryPath
                    + " to the sequence file " + sequenceFilePath);

            docToSeqFileWriter.loadDocumentsToSequenceFile(docDirectoryPath,
                    sequenceFilePath);

            break;
        }

        case 2: {

            String sequenceFilePath = args[1];

            System.out.println("Reading the sequence file " + sequenceFilePath);

            docToSeqFileWriter.readSequenceFile(sequenceFilePath);

            break;
        }

        }

    }

    private void readSequenceFile(String sequenceFilePath) throws IOException {
        // TODO Auto-generated method stub

        /*
         * SequenceFile.Reader sequenceFileReader = new SequenceFile.Reader(fs,
         * new Path(sequenceFilePath), conf);
         */
        Option filePath = SequenceFile.Reader.file(new Path(sequenceFilePath));
        SequenceFile.Reader sequenceFileReader = new SequenceFile.Reader(conf,
                filePath);

        Writable key = (Writable) ReflectionUtils.newInstance(
                sequenceFileReader.getKeyClass(), conf);
        Writable value = (Writable) ReflectionUtils.newInstance(
                sequenceFileReader.getValueClass(), conf);

        try {

            while (sequenceFileReader.next(key, value)) {

                //System.out.printf("[%s] %s %s \n",
                //                sequenceFileReader.getPosition(), key,
                //                value.getClass());
            }
        } finally {
            IOUtils.closeStream(sequenceFileReader);
        }

    }

    private void loadDocumentsToSequenceFile(String docDirectoryPath,
            String sequenceFilePath) throws IOException {
        // TODO Auto-generated method stub

        File docDirectory = new File(docDirectoryPath);

        if (!docDirectory.isDirectory()) {
            System.out
                    .println("Please provide an absolute path of a directory that contains the documents to be added to the sequence file");
            return;
        }

        /*
         * SequenceFile.Writer sequenceFileWriter =
         * SequenceFile.createWriter(fs, conf, new Path(sequenceFilePath),
         * Text.class, BytesWritable.class);
         */
        org.apache.hadoop.io.SequenceFile.Writer.Option filePath = SequenceFile.Writer
                .file(new Path(sequenceFilePath));
        org.apache.hadoop.io.SequenceFile.Writer.Option keyClass = SequenceFile.Writer
                .keyClass(Text.class);
        org.apache.hadoop.io.SequenceFile.Writer.Option valueClass = SequenceFile.Writer
                .valueClass(BytesWritable.class);

        SequenceFile.Writer sequenceFileWriter = SequenceFile.createWriter(
                conf, filePath, keyClass, valueClass);

        File[] documents = docDirectory.listFiles();

        try {
            for (File document : documents) {

                RandomAccessFile raf = new RandomAccessFile(document, "r");
                byte[] content = new byte[(int) raf.length()];

                raf.readFully(content);

                sequenceFileWriter.append(new Text(document.getName()),
                        new BytesWritable(content));

                raf.close();
            }
        } finally {
            IOUtils.closeStream(sequenceFileWriter);
        }

    }
}
