package ru.mai.dep806.bigdata.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static ru.mai.dep806.bigdata.mr.SequenceFileUtils.toSequenceString;

public class TopUsers extends Configured implements Tool {

    static final String[] QUEST_FIELDS = new String[]{
            "CreationDate"
    };

    static final String[] ANSWER_FIELDS = new String[]{
            "CreationDate", "OwnerUserId"
    };

    static final String[] USER_FIELDS = new String[]{
            "DisplayName"
    };

    enum RecordType {NA, Quest, Answer, Post, User}

    private static class TextWithType implements Writable {
        public TextWithType() {
            this(RecordType.NA);
        }

        TextWithType(RecordType recordType) {
            this.recordType = recordType;
            this.record = new Text();
        }

        private RecordType recordType;
        private Text record;

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(recordType.ordinal());
            record.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            recordType = RecordType.values()[in.readInt()];
            record.readFields(in);
        }

        RecordType getRecordType() {
            return recordType;
        }

        Text getRecord() {
            return record;
        }
    }

    private static class PostsPostsMapper extends Mapper<Object, Text, LongWritable, TextWithType> {
//      1. Id of post
//      2. Creation Date of Question
//      3. Creation Date of Answer
//      4. Owner Id of Answer
        private LongWritable outKey1 = new LongWritable();
        private TextWithType outValue1 = new TextWithType(RecordType.Quest);
        private String keyString1, keyString2, typeId;
        private LongWritable outKey2 = new LongWritable();
        private TextWithType outValue2 = new TextWithType(RecordType.Answer);
        private static boolean isLong(String x) {
            try{
                Long.parseLong(x);
            }
            catch (Exception e){
                return false;
            }
            return true;
        }
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());

            keyString1 = row.get("AcceptedAnswerId");
            keyString2 = row.get("Id");
            typeId = row.get("PostTypeId");

            if ("1".equals(typeId) && StringUtils.isNotBlank(keyString1)) {
                outKey1.set(Long.parseLong(keyString1));
                outValue1.getRecord().set(toSequenceString(row, QUEST_FIELDS));
                context.write(outKey1, outValue1);
            } else
                if ("2".equals(typeId) && isLong(row.get("OwnerUserId"))) {
                    outKey2.set(Long.parseLong(keyString2));
                    outValue2.getRecord().set(toSequenceString(row, ANSWER_FIELDS));
                    context.write(outKey2, outValue2);
                }
        }
    }

    static class PostsPostsReducer extends Reducer<LongWritable, TextWithType, LongWritable, Text> {
        private Text outValue = new Text();
        private StringBuilder buffer = new StringBuilder();
        @Override
        protected void reduce(LongWritable key, Iterable<TextWithType> values, Context context) throws IOException, InterruptedException {
            List<String> questions = new ArrayList<>();
            List<String> answers = new ArrayList<>();
            for (TextWithType value : values) {
                String strValue = value.getRecord().toString();
                switch (value.getRecordType()) {
                    case Quest:
                        questions.add(strValue);
                        break;
                    case Answer:
                        answers.add(strValue);
                        break;
                    default:
                        throw new IllegalStateException("Unknown type: " + value.getRecordType());
                }
            }
            if (questions.size() > 0 && answers.size() > 0) {
                for (String question : questions) {
                    for (String answer : answers) {
                        buffer.setLength(0);
                        buffer.append(question).append(answer);
                        outValue.set(buffer.toString());
                        context.write(key, outValue);
                    }
                }
            }
        }
    }

// Может получится, так что поля класса которые я использую внутри маппера надо объявлять внутри маппера.
    private static class PostsMapper extends Mapper<Object, Text, LongWritable, TextWithType> {
//      0. Creation Date of Question
//      1. Creation Date of Answer
//      2. Owner Id of Answer
        static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

        private LongWritable outKeyPost = new LongWritable();
        private TextWithType outValuePost = new TextWithType(RecordType.Post);
        private String[] str;
        private double time;

        private double DateToSec(String creationDateString){
            try {
                Date creationDate = dateFormat.parse(creationDateString);
                return ((double)creationDate.getTime())/1000;
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return 0;
        }
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            str = value.toString().replaceAll("\t", "\u0001").split("\u0001");
            try {
                outKeyPost.set(Long.parseLong(str[3]));
                time = DateToSec(str[2]) - DateToSec(str[1]);
                outValuePost.getRecord().set(String.valueOf(time));
                if(time >= 5) {
                    context.write(outKeyPost, outValuePost);
                }
            } catch (Exception e) {
                throw new InterruptedException("str: " + value.toString());
            }
        }
    }

    private static class UsersMapper extends Mapper<Object, Text, LongWritable, TextWithType> {

        private LongWritable outKeyUser = new LongWritable();
        private TextWithType outValueUser = new TextWithType(RecordType.User);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> row = XmlUtils.parseXmlRow(value.toString());
            String keyString = row.get("Id");
            if (StringUtils.isNotBlank(keyString)) {
                outKeyUser.set(Long.parseLong(keyString));
                outValueUser.getRecord().set(toSequenceString(row, USER_FIELDS));
                context.write(outKeyUser, outValueUser);
            }
        }
    }

    static class PostsUsersReducer extends Reducer<LongWritable, TextWithType, LongWritable, Text> {
        private Text outValue = new Text();
        private StringBuilder buffer = new StringBuilder();

        @Override
        protected void reduce(LongWritable key, Iterable<TextWithType> values, Context context) throws IOException, InterruptedException {
            List<String> posts = new ArrayList<>();
            List<String> users = new ArrayList<>();
            for (TextWithType value : values) {
                String strValue = value.getRecord().toString();
                switch (value.getRecordType()) {
                    case User:
                        users.add(strValue);
                        break;
                    case Post:
                        posts.add(strValue);
                        break;
                    default:
                        throw new IllegalStateException("Unknown type: " + value.getRecordType());
                }
            }
            int count = 0;
            double sum = 0.0;
            String[] str;
            if (users.size() > 0 && posts.size() > 0) {
                for (String user : users) {
                    for (String post : posts) {
                        str = post.replaceAll("\t", "\u0001").split("\u0001");
                        count += 1;
                        sum += Double.parseDouble(str[0]);
                    }
                    if (count != 0)
                        sum = (sum / count);

                    buffer.setLength(0);
                    buffer.append(user).append(sum);
                    outValue.set(buffer.toString());
                    context.write(key, outValue);
                }
            }
        }
    }

    private static class ValueReducer extends Reducer<DoubleWritable, Text, Text, NullWritable> {
        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, NullWritable.get());
            }
        }
    }

    private static class CreateDateMapper extends Mapper<Object, Text, DoubleWritable, Text> {
        private String[] str;
        private DoubleWritable outKey = new DoubleWritable();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            str = value.toString().replaceAll("\t", "\u0001").split("\u0001");
            outKey.set(Double.parseDouble(str[2]));
            outValue.set(str[1] +"\u0001"+ str[0]+"\u0001"+str[2]);
            context.write(outKey, outValue);
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Path outputPath = new Path("/user/stud/andrew/output/PostsPosts");

        Path inputPath = new Path("/user/stud/stackoverflow/landing/Posts");

        Path usersPath = new Path("/user/stud/stackoverflow/landing/Users");

        Path usersTimePath = new Path("/user/stud/andrew/output/users-time");

        Path partitionsFile = new Path("/user/stud/andrew/output/partition");

        Path finalPath = new Path("/user/stud/andrew/output/final");

        Path stagingPath = new Path("/user/stud/andrew/output/stage");

//      job 1
        Job job = Job.getInstance(getConf(), "Join Posts and Posts");
        job.setJarByClass(TopUsers.class);
        job.setReducerClass(PostsPostsReducer.class);
        job.setNumReduceTasks(10);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(TextWithType.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, inputPath, TextInputFormat.class, PostsPostsMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);
        boolean success = job.waitForCompletion(true);
//      job 2
        if (success) {
            Job jobUsers = Job.getInstance(getConf(), "Join Posts and Users + Agragate");
            jobUsers.setJarByClass(TopUsers.class);
            jobUsers.setReducerClass(PostsUsersReducer.class);
            jobUsers.setNumReduceTasks(5);
            jobUsers.setMapOutputKeyClass(LongWritable.class);
            jobUsers.setMapOutputValueClass(TextWithType.class);
            jobUsers.setOutputKeyClass(LongWritable.class);
            jobUsers.setOutputValueClass(Text.class);
            MultipleInputs.addInputPath(jobUsers, outputPath, TextInputFormat.class, PostsMapper.class);
            MultipleInputs.addInputPath(jobUsers, usersPath, TextInputFormat.class, UsersMapper.class);
            FileOutputFormat.setOutputPath(jobUsers, usersTimePath);
            success = jobUsers.waitForCompletion(true);
        }
        if (success) {
            Configuration conf = new Configuration();
            Job analysisJob = Job.getInstance(conf, "Stage 1/2 Analysis");
            analysisJob.setJarByClass(TopUsers.class);
            analysisJob.setMapperClass(TopUsers.CreateDateMapper.class);
            analysisJob.setNumReduceTasks(0);
            analysisJob.setOutputKeyClass(DoubleWritable.class);
            analysisJob.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(analysisJob, usersTimePath);
            analysisJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputPath(analysisJob, stagingPath);
            success = analysisJob.waitForCompletion(true);
        }

        if (success) {
            Job jobSort = Job.getInstance(getConf(), "Sorting Users by time");
            jobSort.setJarByClass(TopUsers.class);
            jobSort.setMapperClass(Mapper.class);
            jobSort.setReducerClass(ValueReducer.class);
            jobSort.setNumReduceTasks(5);
            jobSort.setPartitionerClass(TotalOrderPartitioner.class);
            TotalOrderPartitioner.setPartitionFile(jobSort.getConfiguration(), partitionsFile);
            jobSort.setOutputKeyClass(DoubleWritable.class);
            jobSort.setOutputValueClass(Text.class);
            jobSort.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.setInputPaths(jobSort, stagingPath);
            TextOutputFormat.setOutputPath(jobSort, finalPath);
            InputSampler.RandomSampler<Object, Object> sampler = new InputSampler.RandomSampler<>(0.01, 10000, 10);
            jobSort.getConfiguration().set("io.seqfile.compression.type", SequenceFile.CompressionType.NONE.name());
            InputSampler.writePartitionFile(jobSort, sampler);
            success = jobSort.waitForCompletion(true);
        }

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int result = ToolRunner.run(conf, new TopUsers(), args);
        System.exit(result);
    }
}
