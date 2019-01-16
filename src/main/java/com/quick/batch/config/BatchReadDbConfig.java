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
 * @Description: �����ݿ��ļ���ȡ��txt
 * @ClassName: BatchConfig.java
 * @author: ren.zhen
 * @date: 2018��12��29��
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

	/* 1������һ��Job��ҵ */
	@Bean
	public org.springframework.batch.core.Job  fileReaderJob() throws Exception {
		return   jobBuilderFactory.get("batchAccountDbJob").start(chunkStep()).build();
	}

	// 2������һ��step*/
	@Bean
	public Step chunkStep() throws Exception {
		return stepBuilderFactory.get("batchAccountDbStep").<String,String>chunk(100) // ÿ4���ύһ��
				.reader(DbItemReader()) // ��ȡ�ļ��������ļ���ÿ������ӳ�䵽�����е�User bean��
				.writer(txtItemWriter()).allowStartIfComplete(true).build();
	}


	/**
	 * @Title: txtItemReader
	 * @author ren.zhen
	 * @Description: �����ļ��xȡ
	 * @param @return  ����˵��
	 * @return FlatFileItemReader<BaseMerch>    ��������
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
	//4������д
	@Bean
	public ItemWriter<String> txtItemWriter() throws Exception{

		FlatFileItemWriter<String> fileWrite = new FlatFileItemWriter<>();
		fileWrite.setLineAggregator(new UserLineAggregator());  //�������ݾۺ��������ݰ�ʲô��ʽд���ļ�
		String workFolder = System.getProperty("user.dir");     //��ȡ����Ŀ¼
		fileWrite.setResource(new FileSystemResource(workFolder.concat("/txt/UserWriter.txt")));
		System.out.println(new FileSystemResource(workFolder.concat("/txt/UserWriter.txt")));
		fileWrite.afterPropertiesSet();     //��������������У�飬�������ò���ȷʱ�����쳣
		return fileWrite;
	}

	/*��ָ����ʽд���ļ�*/
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
//		String workFolder = System.getProperty("user.dir");     //��ȡ����Ŀ¼
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
