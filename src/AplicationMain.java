// Aluno: Carlos Eduardo Rodrigues
// Aluno: Nathan Machado Josviak
import com.google.common.collect.Lists;
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


public class AplicationMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c,args).getRemainingArgs();
        Path input = new Path(files[0]);

        TesteExercicio(1, input, c);
        TesteExercicio(2, input, c);
       TesteExercicio(3, input, c);
       TesteExercicio(4, input, c);
        TesteExercicio(5, input, c);

    } // public static void

    public static void TesteExercicio(int IdExercicio, Path input, Configuration c) throws IOException, ClassNotFoundException, InterruptedException {
        Job j = new Job(c,"ApplicationMain");
        j.setJarByClass(AplicationMain.class);
        switch (IdExercicio){
            case 1:
                j.setMapperClass(MapBrazilTransactions.class);
                j.setReducerClass(ReduceBrazilTransactions.class);
                j.setMapOutputKeyClass(Text.class);
                j.setMapOutputValueClass(IntWritable.class);
                j.setOutputKeyClass(Text.class);
                j.setOutputValueClass(IntWritable.class);
                break;
            case 2:
                j.setMapperClass(MapYearTransactions.class);
                j.setReducerClass(ReduceYearTransactions.class);
                j.setMapOutputKeyClass(Text.class);
                j.setMapOutputValueClass(IntWritable.class);
                j.setOutputKeyClass(Text.class);
                j.setOutputValueClass(IntWritable.class);
                break;
            case 3:
                j.setMapperClass(MapTransactionsPerFlowAndYear.class);
                j.setReducerClass(ReduceTransactionsPerFlowAndYear.class);
                j.setMapOutputKeyClass(Text.class);
                j.setMapOutputValueClass(IntWritable.class);
                j.setOutputKeyClass(Text.class);
                j.setOutputValueClass(IntWritable.class);
                break;
            case 4:
                j.setMapperClass(MapAverageCommodityValuePerYear.class);
                j.setReducerClass(ReduceAverageCommodityValuePerYear.class);
                j.setMapOutputKeyClass(Text.class);
                j.setMapOutputValueClass(IntWritable.class);
                j.setOutputKeyClass(Text.class);
                j.setOutputValueClass(DoubleWritable.class);
                break;
            case 5:
                j.setMapperClass(MapAveragePricePerUnitYearExportBrazil.class);
                j.setReducerClass(ReduceAveragePricePerUnitYearExportBrazil.class);
                j.setMapOutputKeyClass(Text.class);
                j.setMapOutputValueClass(IntWritable.class);
                j.setOutputKeyClass(Text.class);
                j.setOutputValueClass(DoubleWritable.class);
                break;
        }
        FileInputFormat.addInputPath(j,input);
        FileOutputFormat.setOutputPath(j,new Path("output/TDE1/Exercicio_"+IdExercicio));
        j.waitForCompletion(false);
    }


    /* ------------------------------------ [ EXERCICIO 1 ] ------------------------------------ */
    public static class MapBrazilTransactions extends Mapper<LongWritable, Text, Text, IntWritable>{
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
            String[] atributos = value.toString().split(";");
            con.write(new Text(atributos[0]), new IntWritable(1));
        }
    } // static class MapBrazilTransactions
    public static class ReduceBrazilTransactions extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> ocorrencias, Context con) throws IOException, InterruptedException{
            if (key.toString().equals("Brazil")){
                con.write(
                        new Text("Brazil Transactions: "),
                        new IntWritable(Lists.newArrayList(ocorrencias).size())
                );
            }
        }
    } // static class ReduceBrazilTransactions


    /* ------------------------------------ [ EXERCICIO 2 ] ------------------------------------ */
    public static class MapYearTransactions extends Mapper<LongWritable, Text, Text, IntWritable>{
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
            String[] atributos = value.toString().split(";");
            con.write(new Text(atributos[1]), new IntWritable(1));
        }
    } // static class MapYearTransactions
    public static class ReduceYearTransactions extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> ocorrencias, Context con) throws IOException, InterruptedException{
            con.write(
                    new Text("Transactions in "+key.toString()+": "),
                    new IntWritable(Lists.newArrayList(ocorrencias).size())
            );
        }
    } // static class ReduceYearTransactions


    /* ------------------------------------ [ EXERCICIO 3 ] ------------------------------------ */
    public static class MapTransactionsPerFlowAndYear extends Mapper<LongWritable, Text, Text, IntWritable>{
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
            String[] atributos = value.toString().split(";");
            String k = atributos[1] + ";" + atributos[4];
            con.write(new Text(k), new IntWritable(1));
        }
    } // static class MapTransactionsPerFlowAndYear
    public static class ReduceTransactionsPerFlowAndYear extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> ocorrencias, Context con) throws IOException, InterruptedException{
            String keys = key.toString().split(";")[1] + " transactions in year " + key.toString().split(";")[0]+": ";
            con.write(
                    new Text(keys),
                    new IntWritable(Lists.newArrayList(ocorrencias).size())
            );
        }
    }

    public static class MapAverageCommodityValuePerYear extends Mapper<LongWritable, Text, Text, IntWritable>{
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
            String[] atributos = value.toString().split(";");
            String k = atributos[1] + ";" + atributos[2];
            int val = new Integer(atributos[5]);
            con.write(new Text(k), new IntWritable(val));
        }
    }
    public static class ReduceAverageCommodityValuePerYear extends Reducer<Text, IntWritable, Text, DoubleWritable>{
        public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException{
            String keys = "Average value of commodity " + key.toString().split(";")[1] + " in year " + key.toString().split(";")[0]+": ";
            double value = 0.0;
            for (IntWritable val : values){
                value += Integer.valueOf(val.toString());
            }
            value = value/Lists.newArrayList(values).size();
            con.write(
                    new Text(keys),
                    new DoubleWritable(value)
            );
        }
    } // static class ReduceTransactionsPerFlowAndYear


    /* ------------------------------------ [ EXERCICIO 5 ] ------------------------------------ */

    public static class MapAveragePricePerUnitYearExportBrazil extends Mapper<LongWritable, Text, Text, IntWritable>{
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException{
            String[] atributos = value.toString().split(";");
            String k = atributos[1] + ";" + atributos[4] + ";" + atributos[7];
            con.write(new Text(k), new IntWritable(Integer.valueOf(atributos[5])));
        }
    } // static class MapAveragePricePerUnitYearExportBrazil
    public static class ReduceAveragePricePerUnitYearExportBrazil extends Reducer<Text, IntWritable, Text, DoubleWritable>{
        public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException{
            String[] atributos = key.toString().split(";");
            String message = "Average price of all "+atributos[1]+"ed "+atributos[2]+" of commodities in year "+atributos[0];
            double average = 0.0;
            for (IntWritable value : values){
                average += Double.parseDouble(value.toString());
            }
            average = average / Lists.newArrayList(values).size();
            con.write(
                    new Text(message),
                    new DoubleWritable(average)
            );
        }
    } // static class ReduceAveragePricePerUnitYearExportBrazil

} // class ApplicationMain


