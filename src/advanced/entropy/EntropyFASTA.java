package advanced.entropy;

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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Base64;
import java.util.LinkedList;
import java.util.Locale;

public class EntropyFASTA {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        Path intermediate = new Path("./output/intermediate.tmp");

        // arquivo de saida
        Path output = new Path(files[1]);

       Job j1 = new Job(c, "etapa1");

       //registro de classes
        j1.setJarByClass(EntropyFASTA.class);
        j1.setMapperClass(MapEtapaA.class);
        j1.setReducerClass(ReduceEtapaA.class);

        // definicao dos tipos de saida
        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(LongWritable.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(LongWritable.class);

        //Definir os arquivos de entrada e saida
        FileInputFormat.addInputPath(j1,input);
        FileOutputFormat.setOutputPath(j1, intermediate);

        //rodando o job 1
        j1.waitForCompletion(false);

        // criando o job 2

        Job j2 = new Job(c, "etapa 2");

        //registro de classes
    j2.setJarByClass(EntropyFASTA.class);
    j2.setMapperClass(MapEtapaB.class);
    j2.setReducerClass(ReduceEtapaB.class);

        // definicao dos tipos de saida
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(BaseQtdWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(DoubleWritable.class);

        //arquivos de entrada e saida
        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);
    }

    public static class MapEtapaA extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // pegando uma linha do arquivo

            String linha = value.toString();

            // ignorando cabeçalho

            if (!linha.startsWith(">")){
                String caracteres[] = linha.split("");
                // gerando (chave = caracter, valor=1)
                for (String c : caracteres){
                    con.write(new Text(c),
                              new LongWritable(1));
                }
            }

        }
    }

    public static class ReduceEtapaA extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {

            // somando as ocorrencias do caracter

            long soma = 0;

            for (LongWritable v : values){
                soma = v.get();
            }
            con.write(key, new LongWritable(soma));


        }
    }


    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, BaseQtdWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            //obter linha
            String linha = value.toString();
            //obter os campos
            String campos[] = linha.split("\t");

            con.write(new Text("todos"), new BaseQtdWritable(campos[0],Long.parseLong(campos[1])));


        }
    }

    public static class ReduceEtapaB extends Reducer<Text, BaseQtdWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<BaseQtdWritable> values, Context con)
                throws IOException, InterruptedException {
            LinkedList<BaseQtdWritable> valores = new LinkedList<>();
            long total = 0;

            for(BaseQtdWritable v : valores){
                total += v.getContagem();
                valores.add(new BaseQtdWritable(v.getCaracter(),
                        v.getContagem()));
            }

            // calcular probablidade e
            // entropia de cada caracter

            for (BaseQtdWritable v :valores){
                double p = v.getContagem() / (double) total;
                double entropia = -p *  Math.log(p) / Math.log(2.0);
                con.write(new Text(v.getCaracter()), new DoubleWritable(entropia));

            }
        }
    }
}
