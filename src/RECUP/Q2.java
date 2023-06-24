package RECUP;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class Q2 {

    // Main
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Job j = Job.getInstance(c, "Q2");

        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        j.setJarByClass(Q2.class);
        j.setMapperClass(Q2Mapper.class);
        j.setCombinerClass(Q2Combiner.class); // Combiner
        j.setReducerClass(Q2Reducer.class);

        // Tipos de saida
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class); // valores em dolars

        // Arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(false);
    }

    // Map
    public static class Q2Mapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text category = new Text();
        private DoubleWritable transactionValue = new DoubleWritable(); // valor em dolar

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            if (((Object) key).toString().equals("0")) {
                return; // Ignora o cabeçalho do CSV
            }

            String[] parts = value.toString().split(";"); // separa por ;
            String categoryValue = parts[9]; // pegar o pedaço 9
            double transactionUSDValue = Double.parseDouble(parts[5]); // pega o pedaço 5 converte de string pra double

            category.set(categoryValue);
            transactionValue.set(transactionUSDValue);

            // define o par de chave-valor
            context.write(category, transactionValue);
        }
    }

    // Combiner
    public static class Q2Combiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            // loop para percorrer os valores doublewritable
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            // faz a media dos valores
            double average = sum / count;

            result.set(average);
            context.write(key, result);
        }
    }

    // Reduce
    public static class Q2Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            double average = sum / count;

            result.set(average);
            context.write(key, result);
        }
    }

}
