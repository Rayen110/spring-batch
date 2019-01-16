package com.quick.batch.config;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.quick.batch.model.RecordSO;
import com.quick.batch.model.User;
import com.quick.batch.model.WriterSO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

//@Configuration
//@EnableBatchProcessing
public class BatchConfig1 {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchConfiguration.class);


    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    private int chunkSize = 5;

    @Autowired
    private   DataSource dataSource;


    /*1、创建一个Job作业*/
    @Bean
    public Job xmlReaderJob() throws Exception{
        return jobBuilderFactory.get("xmlsReaderJob")
        .start(chunkStep())
        .build();
    }

    //2、创建一个step*/
    @Bean
    public Step chunkStep() throws Exception{
        return stepBuilderFactory.get("chunkStep")
                .<RecordSO, RecordSO>chunk(chunkSize)                                   //每chunkSize次提交一次
                .reader(reader())                            //读取xml文件，并把文件中每个标签中数据映射到工程中的User bean中
                .writer(userWriteItem())
                .allowStartIfComplete(true)
                .build();
    }
//    @Bean
    public ItemReader<RecordSO> reader() {

        /**
         *  mysql 分页查询
         *  JdbcPagingItemReader<DataQualityResume> reader = new JdbcPagingItemReader<>();
         *  MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
         *  queryProvider.setSelectClause("select *");
         *  queryProvider.setFromClause("from data_quality_resume");
         *  Map<String,Order> keys = new HashMap<>();
         *  keys.put("created",Order.DESCENDING);
         *  queryProvider.setSortKeys(keys);
         *  reader.setQueryProvider(queryProvider);
         *  reader.setPageSize(10000);
         *  reader.setDataSource(dataSource);
         */

        JdbcCursorItemReader<RecordSO> reader = new JdbcCursorItemReader<>();
        reader.setSql("select id, firstName, lastname, random_num from reader");
        reader.setDataSource(dataSource);
        reader.setRowMapper(
                (ResultSet resultSet, int rowNum) -> {
                    if (!(resultSet.isAfterLast()) && !(resultSet.isBeforeFirst())) {
                        RecordSO recordSO = new RecordSO();
                        recordSO.setFirstName(resultSet.getString("firstName"));
                        recordSO.setLastName(resultSet.getString("lastname"));
                        recordSO.setId(resultSet.getInt("Id"));
                        recordSO.setRandomNum(resultSet.getString("random_num"));

                        LOGGER.info("RowMapper record : {}", recordSO);
                        return recordSO;
                    } else {
                        LOGGER.info("Returning null from rowMapper");
                        return null;
                    }
                });
        return reader;
    }
    //3、配置要读取文件的特性*/
//    @Bean
//    public ItemReader<WriterSO> xmlItemReader(){
//        StaxEventItemReader<WriterSO> reader = new StaxEventItemReader<>(); //StaxEventItemReader用来读取xml文件
//        reader.setResource(new ClassPathResource("/data/User.xml"));    //设置xml文件位置
//        reader.setFragmentRootElementName("user");  //指定xml文件的根元素，或者在User.java类上用@XmlRootElement(name = "user")
//        reader.setUnmarshaller(getMarShaller());    //把xml文件中数据映射到User.java中
//        return reader;
//    }

//    private XStreamMarshaller getMarShaller(){
//        XStreamMarshaller marShaller = new XStreamMarshaller();
//        Map<String, Class> map = new HashMap<>();
//        map.put("user", User.class); //把<user>标签中数据映射到User.class类中
//        marShaller.setAliases(map);
//        return marShaller;
//    }


    //4、配置写
    @Bean
    public ItemWriter<RecordSO> userWriteItem() throws Exception{

        FlatFileItemWriter<RecordSO> fileWrite = new FlatFileItemWriter<>();
        fileWrite.setLineAggregator(new UserLineAggregator());  //设置数据聚合器，数据按什么格式写入文件
        String workFolder = System.getProperty("user.dir");     //获取工程目录
        fileWrite.setResource(new FileSystemResource(workFolder.concat("/txt/UserWriter.txt")));
        System.out.println(new FileSystemResource(workFolder.concat("/txt/UserWriter.txt")));
        fileWrite.afterPropertiesSet();     //设置完参数后进行校验，参数设置不正确时会抛异常
        return fileWrite;
    }

    /*按指定格式写入文件*/
    private class UserLineAggregator implements LineAggregator<RecordSO> {
        @Override
        public String aggregate(RecordSO item) {
            System.out.print("-----------------"+item.getId() + ", " + item.getLastName()+ ", " + item.getRandomNum());
            return item.getId() + ", " + item.getLastName()+ ", " + item.getRandomNum();
        }
    }




}