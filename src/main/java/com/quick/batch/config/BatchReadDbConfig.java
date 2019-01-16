package com.quick.batch.config;

import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.quick.batch.model.RecordSO;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;


/**
 * @Description: 把数据库文件读取到txt
 * @ClassName: BatchConfig.java
 * @author: ren.zhen
 * @date: 2018年12月29日
 * @Copyright:  2018 www.sandpay.com.cn Inc. All rights reserved.
 */
@Configuration
@EnableBatchProcessing
public class BatchReadDbConfig {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private JobBuilderFactory jobBuilderFactory;


	@Autowired
	private  DataSource dataSource;

	/* 1、创建一个Job作业 */
	@Bean
	public org.springframework.batch.core.Job  fileReaderJob() throws Exception {
		return   jobBuilderFactory.get("batchAccountDbJob").start(chunkStep()).build();
	}

	// 2、创建一个step*/
	@Bean
	public Step chunkStep() throws Exception {
		return stepBuilderFactory.get("batchAccountDbStep").<String,String>chunk(100) // 每4次提交一次
				.reader(DbItemReader()) // 读取文件，并把文件中每行数据映射到工程中的User bean中
				.writer(txtItemWriter()).allowStartIfComplete(true).build();
	}


	/**
	 * @Title: txtItemReader
	 * @author ren.zhen
	 * @Description: 批量文件讀取
	 * @param @return  参数说明
	 * @return FlatFileItemReader<BaseMerch>    返回类型
	 * @throws
	 */
	@Bean
	public ItemReader<String> DbItemReader() {
 	JdbcCursorItemReader<String> itemReader = new JdbcCursorItemReader<String>();
		itemReader.setDataSource(dataSource);
		itemReader.setSql("  SELECT CONCAT(mer_no,'|',mp_no,'|',account_no,'|',user_no) as str FROM br_sign_mp_accout ");
		itemReader.setRowMapper(new  RowMapper<String>() {
			String result = "";
			@Override
			public String mapRow(ResultSet rs, int rowNum) throws SQLException {
				result = rs.getString("str");
				System.out.println("ResultSet:"+rs);
				System.out.println("result:"+result);
				return result;
			}
		});
		ExecutionContext executionContext = new ExecutionContext();
		itemReader.open(executionContext);
		Object customerCredit = new Object();
		while(customerCredit != null){
			try {
				customerCredit = itemReader.read();
				System.out.println("customerCredit:"+customerCredit);
			}   catch (Exception e) {
				e.printStackTrace();
			}
		}
		executionContext.containsValue(customerCredit);
		itemReader.close();
		return itemReader;

	}
	//4、配置写
	@Bean
	public ItemWriter<String> txtItemWriter() throws Exception{

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
			System.out.print("-----------item------"+item);
			return item;
		}
	}
//	@Bean
//	public ItemWriter<String> txtItemWriter() {
//		FlatFileItemWriter<String> txtItemWriter = new FlatFileItemWriter<>();
//		txtItemWriter.setLineAggregator(new LineAggregator<String>() {
//
//			@Override
//			public String aggregate(String item) {
//				System.out.println("item:"+item);
//				return item;
//			}
//		});
//		txtItemWriter.setAppendAllowed(true);
//		txtItemWriter.setEncoding("UTF-8");
//		String workFolder = System.getProperty("user.dir");     //获取工程目录
//		System.out.println(new FileSystemResource(workFolder.concat("/txt/UserWriter.txt")));
//		txtItemWriter.setResource(new FileSystemResource(workFolder.concat("/txt/UserWriter.txt")));
//		try {
//			txtItemWriter.afterPropertiesSet();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return txtItemWriter;
//	}
}
