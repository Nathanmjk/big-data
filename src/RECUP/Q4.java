package RECUP;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class Q4 {

    // Main
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Job j = Job.getInstance(c, "Q4");

        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        j.setJarByClass(Q4.class);
        j.setMapperClass(Q4Mapper.class);
        j.setCombinerClass(Q4Combiner.class);
        j.setReducerClass(Q4Reducer.class);

        // Tipos de saida
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        // Arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(false);
    }

    // Map
    public static class Q4Mapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text country = new Text();
        private DoubleWritable transactionValue = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (((Object) key).toString().equals("0")) {
                return; // Ignora o cabeçalho do CSV
            }

            // separa por ;
            String[] parts = value.toString().split(";");

            // Obtém o fluxo  e o valor da transação
            String flow = parts[4];
            double transactionUSDValue = Double.parseDouble(parts[5]);

            // Verifica se o fluxo é Export,se for ele seta a chave = país e o valor = valor da transação
            if (flow.equals("Export")) {
                String countryValue = parts[0];
                country.set(countryValue);
                transactionValue.set(transactionUSDValue);

                context.write(country, transactionValue);
            }
        }
    }

    // Combine
    public static class Q4Combiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            double average = sum / count;

            context.write(key, new DoubleWritable(average));
        }
    }



    // Reduce
    public static class Q4Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private Text resultCountry = new Text();
        private DoubleWritable resultValue = new DoubleWritable(Double.MIN_VALUE);

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }

            double average = sum / count;

            if (average > resultValue.get()) {
                resultCountry.set(key);
                resultValue.set(average);
            }
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(resultCountry, resultValue);
        }
    }

}
