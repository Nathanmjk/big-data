package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AverageTemperature {

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "media");

        // Registro de classes
        j.setJarByClass(AverageTemperature.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);
        j.setCombinerClass(CombineForAverage.class);

        // Tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FireAvgTempWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        // Arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // rodar :)
        j.waitForCompletion(false);

    }


    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgTempWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // obtendo a linha do arquivo
            String linha = value.toString();

            // quebrando em colunas
            String colunas[] = linha.split(",");

            // obtendo a temperatura
            float temperatura = Float.parseFloat(colunas[8]);

            // Criando chave unica
            Text chave = new Text("global");

            // Criando (chave = "global", valor=(soma=temperatura, n=1))
            con.write(chave, new FireAvgTempWritable(temperatura, 1));

        }
    }

    public static class CombineForAverage extends Reducer<Text, FireAvgTempWritable, Text, FireAvgTempWritable>{
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {

            int somaQtd = 0;

            float somaTemp = 0.0f;

            for(FireAvgTempWritable o : values){
                somaQtd += o.getN();
                somaTemp += o.getSomaTemperatura();
            }

            con.write(key, new FireAvgTempWritable(somaQtd,  somaTemp));



        }
    }


    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable,
            Text, FloatWritable> {
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {

            // somando as somas de temperatura e a soma das quantidades
            float somaTemperatura = 0.0f;
            int somaQuantidades = 0;
            for(FireAvgTempWritable o : values){
                somaTemperatura += o.getSomaTemperatura();
                somaQuantidades += o.getN();
            }

            // calculando a media
            float media = somaTemperatura / somaQuantidades;

            // salvando o resultado (chave = "global", valor = media)
            con.write(key, new FloatWritable(media));




        }
    }

}
