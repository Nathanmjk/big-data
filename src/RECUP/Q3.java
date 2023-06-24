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

public class Q3 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        Job j = new Job(c, "Q3");

        // Registro de classes
        j.setJarByClass(Q1.class);
        j.setMapperClass(MapForQ1.class);
        j.setCombinerClass(CombinerForQ1.class); // Define como combiner
        j.setReducerClass(ReduceForQ1.class);

        // Tipos de saida
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);


        // Arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(false);
    }

    // Map
    public static class MapForQ1 extends Mapper<Object, Text, Text, DoubleWritable> {

        private DoubleWritable quantity = new DoubleWritable();
        private Text commodity = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (((Object) key).toString().equals("0")) {
                return; // Ignora o cabeçalho do CSV
            }

            String[] linha = value.toString().split(";");

            String year = linha[1].trim();
            String flow = linha[4].trim();

            double quantityValue = Double.parseDouble(linha[8]);

            if (year.equals("2018") && flow.equals("Import")) {
                String commodityName = linha[3];
                commodity.set(commodityName);
                quantity.set(quantityValue);
                context.write(commodity, quantity);
            }
        }
    }

    // Combiner
    public static class CombinerForQ1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double totalQuantity = 0.0;
            for (DoubleWritable value : values) {
                totalQuantity += value.get();
            }
            context.write(key, new DoubleWritable(totalQuantity));
        }
    }

    // Reduce
    public static class ReduceForQ1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double totalQuantity = 0.0;
            for (DoubleWritable value : values) {
                totalQuantity += value.get();
            }
            context.write(key, new DoubleWritable(totalQuantity));
        }
    }
}
