package com.javatechie.spring.batch.config;

import com.javatechie.spring.batch.entity.Email;
import com.javatechie.spring.batch.repository.EmailRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

@Configuration
@EnableBatchProcessing
@AllArgsConstructor
public class SpringBatchConfig {

    private JobBuilderFactory jobBuilderFactory;

    private StepBuilderFactory stepBuilderFactory;

    private EmailRepository emailRepository;


    @Bean
    public FlatFileItemReader<Email> reader() {
        FlatFileItemReader<Email> itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource("src/main/resources/ES210731.txt"));
        itemReader.setName("txtReader");
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());
        return itemReader;
    }

    private LineMapper<Email> lineMapper() {
        DefaultLineMapper<Email> lineMapper = new DefaultLineMapper<>();

//        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
//        lineTokenizer.setDelimiter(",");
//        lineTokenizer.setStrict(false);
//        lineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");
//
//        BeanWrapperFieldSetMapper<Customer> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
//        fieldSetMapper.setTargetType(Customer.class);
//
//        lineMapper.setLineTokenizer(lineTokenizer);
//        lineMapper.setFieldSetMapper(fieldSetMapper);
//
        FixedLengthTokenizer rc = new FixedLengthTokenizer();
        String[] names = new String[]{"field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8", "field9", "field10", "field11","field12"};
        rc.setNames(names);
        Range[] ranges = new Range[]{
                new Range(4, 6),
                new Range(12, 26),
                new Range(29, 32),
                new Range(34, 41),
                new Range(52, 60),
                new Range(62, 68),
                new Range(69, 76),
                new Range(78, 89),
                new Range(90, 114),
                new Range(114, 196),
                new Range(194, 221),
                new Range(221, 260)
        };
        rc.setColumns(ranges);

        lineMapper.setLineTokenizer(rc);

        BeanWrapperFieldSetMapper<Email> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Email.class);


        lineMapper.setFieldSetMapper(fieldSetMapper);
        return lineMapper;

    }

    @Bean
    public EmailProcessor processor() {
        return new EmailProcessor();
    }

    @Bean
    public RepositoryItemWriter<Email> writer() {
        RepositoryItemWriter<Email> writer = new RepositoryItemWriter<>();
        writer.setRepository(emailRepository);
        writer.setMethodName("save");
        return writer;
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("csv-step").<Email, Email>chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public Job runJob() {
        return jobBuilderFactory.get("importCustomers")
                .flow(step1()).end().build();

    }

    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor asyncTaskExecutor = new SimpleAsyncTaskExecutor();
        asyncTaskExecutor.setConcurrencyLimit(10);
        return asyncTaskExecutor;
    }

}
