package RECUP;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
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

public class Q1 {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        Job j = new Job(c, "Q1");

        // Registro de classes
        j.setJarByClass(Q1.class);
        j.setMapperClass(MapForQ1.class);
        j.setCombinerClass(CombinerForQ1.class); // Define combiner
        j.setReducerClass(ReduceForQ1.class);

        // Tipos de saida
        j.setOutputKeyClass(Text.class); // chave tipo text
        j.setOutputValueClass(IntWritable.class); // valor tipo intWritable


        // Arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        j.waitForCompletion(false);
    }

    // Map
    public static class MapForQ1 extends Mapper<Object, Text, Text, IntWritable> {

        // valor definido como 1 pra representa a ocorrencia de uma transação
        private final static IntWritable valor = new IntWritable(1);
        private Text countryAndFlow = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] linha = value.toString().split(";");
            String country = linha[0].trim();
            String flow = linha[4].trim();

            // definição da chave pais + fluxo
            countryAndFlow.set(country + " = " + flow);

            // cria o par de chave-valor
            context.write(countryAndFlow, valor);
        }
    }

    // Combiner
    public static class CombinerForQ1 extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int transactionCount = 0;

            // loop para contar a   o total de transacoes
            for (IntWritable value : values) {
                transactionCount += value.get();
            }

            //gera o par de chave-valor
            context.write(key, new IntWritable(transactionCount));
        }
    }

    // Reduce
    public static class ReduceForQ1 extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int transactionCount = 0;

            // loop para contar o total de transações por chaves
            for (IntWritable value : values) {
                transactionCount += value.get();
            }
            context.write(key, new IntWritable(transactionCount));
        }
    }


}
