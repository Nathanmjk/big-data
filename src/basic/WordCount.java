package basic;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


public class WordCount {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "wordcount");

        // Registro de classes
        j.setJarByClass(WordCount.class); // Classe que tem o método MAIN
        j.setMapperClass(MapForWordCount.class); // Classe do MAP
        j.setReducerClass(ReduceForWordCount.class); // Classe do REDUCE
        j.setCombinerClass(CombineForWordCount.class);

        // Tipos de saida
        j.setMapOutputKeyClass(Text.class); // tipo da chave de saida do MAP
        j.setMapOutputValueClass(IntWritable.class); // tipo do valor de saida do MAP
        j.setOutputKeyClass(Text.class); // tipo de chave de saida do reduce
        j.setOutputValueClass(IntWritable.class); // tipo de valor de saida do reduce

        // Definindo arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input); // adicionando o caminho do input no job
        FileOutputFormat.setOutputPath(j, output); // adicionando o caminho de output no job

        // rodar :)
        j.waitForCompletion(false);

    }


    /**
     * 1o tipo: tipo da chave de entrada (LongWritable)
     * 2o tipo: tipo do valor de entrada (Text)
     * 3o tipo: tipo da chave de saida (palavra, Text)
     * 4o tipo: tipo do valor saida (1, int -> IntWritable)
     */
    public static class MapForWordCount extends Mapper<LongWritable, Text,
            Text, IntWritable> {
        // A chave de entrada do map é um offset (irrelevante)
        // O conteudo do arquivo é lido no value (cada linha do arquivo)
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // obtendo o conteudo da linha
            String linha = value.toString();
            // quebrando a linha em palavras
            String palavras[] = linha.split(" ");
            // loop para gerar chaves e valores (chave=palavra, valor=1)
            for (String p : palavras){
                Text chave = new Text(p);
                IntWritable valor = new IntWritable(1);
                con.write(chave, valor);
            }


        }
    }

    public static class CombineForWordCount extends Reducer<Text, IntWritable,
            Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int sum =0;
            for(IntWritable o : values){
                sum += o.get();

            }
            con.write(key, new IntWritable(sum));

        }
    }




    /**
     * 1o tipo: chave de entrada do reduce (DEVE CASAR COM O TIPO DA CHAVE DE SAIDA DO MAP)
     * 2o tipo: valor de entrada do reduce (DEVE CASAR COM O TIPO DO VALOR DE SAIDA DO MAP)
     * 3o tipo: chave de saida do reduce
     * 4o tipo: valor de saida do reduce
     */
    public static class ReduceForWordCount extends Reducer<Text, IntWritable,
            Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int soma = 0;
            // loop para iterar sobre os valores de entrada e somar eles
            for (IntWritable v : values){
                soma += v.get();
            }
            // salvando o resultado em disco
            con.write(key, new IntWritable(soma));

        }
    }

}
