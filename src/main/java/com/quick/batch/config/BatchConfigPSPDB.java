package com.quick.batch.config;

import java.sql.ResultSet;

import javax.sql.DataSource;

import com.quick.batch.model.RecordSO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

//@Configuration
//@EnableBatchProcessing
public class BatchConfigPSPDB {


    private static final Logger LOGGER = LoggerFactory.getLogger(BatchConfiguration.class);


    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    private int chunkSize = 200000;

    @Autowired
    private   DataSource dataSource;

    private String sql=" SELECT t.acc_str  FROM (SELECT  CONCAT( 'psp|', LPAD((@i:=@i+1), 16, '0' ), '|', DATE_FORMAT( now( ), '%Y%m%H' ), '|0018|', mc.account_org_no, '|', '0001|', mc.account_type, '|', (CASE WHEN mc.fee_charge_mode = '1' THEN '01'  WHEN mc.fee_charge_mode = '2' THEN '11'  WHEN mc.fee_charge_mode = '3' THEN '14' END  ), '|', mc.account_no, '|', a.account_name, '|', bsc.cl_no, '|', mc.user_no, '|', mc.currency, '|', mc.credit_flag, '|', IFNULL( mc.credit_amt, '0.00' ), '|', '20500101|', mc.d0_flag,'|0|', mc.mer_no, '|', bm.merch_reg_name  ) AS acc_str  FROM br_sign_mp_accout mc INNER JOIN base_account a ON mc.account_id = a.account_id INNER JOIN base_sign_contract bsc ON bsc.mer_no = mc.mer_no INNER JOIN base_merch bm ON bm.cl_no = bsc.cl_no  ,(select @i:=0) as it ) t WHERE t.acc_str IS NOT NULL ";

    /*1、创建一个Job作业*/
    @Bean
    public Job pspdbReaderJob() throws Exception{
        return jobBuilderFactory.get("pspdbReaderJob")
                .start(chunkStep())
                .build();
    }

    //2、创建一个step*/
    @Bean
    public Step chunkStep() throws Exception{
        return stepBuilderFactory.get("chunkStep")
                .<String, String>chunk(chunkSize)                                   //每chunkSize次提交一次
                .reader(reader())                            //读取xml文件，并把文件中每个标签中数据映射到工程中的User bean中
                .writer(userWriteItem())
                .allowStartIfComplete(true)
                .build();
    }
    //    @Bean
    public ItemReader<String> reader() {

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

        JdbcCursorItemReader<String> reader = new JdbcCursorItemReader<>();
//        reader.setSql(" SELECT CONCAT(mer_no,'|',mp_no,'|',account_no,'|',user_no) as acc_str FROM br_sign_mp_accout  ");
        reader.setSql(sql);
        reader.setDataSource(dataSource);
        reader.setRowMapper(
                (ResultSet resultSet, int rowNum) -> {
                    if (!(resultSet.isAfterLast()) && !(resultSet.isBeforeFirst())) {
                        resultSet.getString("acc_str") ;


                        return  resultSet.getString("acc_str");
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
    public ItemWriter<String> userWriteItem() throws Exception{

        FlatFileItemWriter<String> fileWrite = new FlatFileItemWriter<>();
        fileWrite.setLineAggregator(new UserLineAggregator());  //设置数据聚合器，数据按什么格式写入文件
        String workFolder = System.getProperty("user.dir");     //获取工程目录
        fileWrite.setResource(new FileSystemResource(workFolder.concat("/txt/UserWriter.txt")));
        System.out.println(new FileSystemResource(workFolder.concat("/txt/UserWriter.txt")));
        fileWrite.afterPropertiesSet();     //设置完参数后进行校验，参数设置不正确时会抛异常
        return fileWrite;
    }

    /*按指定格式写入文件*/
    private class UserLineAggregator implements LineAggregator<String> {
        @Override
        public String aggregate(String item) {
            System.out.println("-----------------"+item);
            return item;
        }
    }




}