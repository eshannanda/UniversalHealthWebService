/* 

 * Copyright 2012-2015 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.client.rest;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;

@Controller
public class RESTController {
	private static Logger log = Logger.getLogger(RESTController.class); 
	
	@Autowired
	AerospikeClient client;
	
	boolean hospitalIndexCreated = false;
	boolean procedureIndexCreated = false;
	boolean claimPackageRefistered = false;
	boolean groupByPackageRefistered = false;

	/**
	 * get a specific claim record via primary key
	 * @param namespace
	 * @param set
	 * @param keyvalue
	 * @return
	 * @throws Exception
	 */
	@RequestMapping(value="/{namespace}/{set}/getAll/{key}", method=RequestMethod.GET)
	public @ResponseBody JSONRecord getAll(@PathVariable("namespace") String namespace, @PathVariable("set") String set, @PathVariable("key") String keyvalue) throws Exception {

		Policy policy = new Policy();
		Key key = new Key(namespace, set, Value.get(keyvalue));
		Record record = client.get(policy, key);
		KeyRecord result = new KeyRecord(key, record);

		return new JSONRecord(record);
	}
	/**
	 * Query and filter by hospital Id
	 * This method assumes a secondary index on "hospital_index" 
	 * has already been created, and the package "claim-package.lua"
	 * has been registered
	 * @param namespace
	 * @param set
	 * @param hospitalId
	 * @param date
	 * @return
	 * @throws Exception
	 */
	@RequestMapping(value="/{namespace}/{set}/getClaimsByHospitalId/{hostpitalId}", method=RequestMethod.GET)
	public @ResponseBody JSONObject getClaimsByHospital(@PathVariable("namespace") String namespace, 
			@PathVariable("set") String set, @PathVariable("hostpitalId") String hospitalId) throws Exception {

		/*
		 * create the data index once
		 */
		if (!hospitalIndexCreated){
			/*
			 * check if the index is created
			 */
			Node[] nodes = client.getNodes();
			String infoResult = Info.request(nodes[0], "sindex");
			/*
			 * create if not found
			 */
			if (!infoResult.contains("hospital_index")){
				log.debug("The index: hospital_index has not been created, creating now...");
				IndexTask task = this.client.createIndex(null, namespace, set, "hospital_index", "Hospital", IndexType.STRING);
				task.waitTillComplete();
				log.debug("The index: claim_date_index created successfully");
			}
			hospitalIndexCreated = true;
		}
		/*
		 * register claim package once
		 */
		if (!claimPackageRefistered){
			/*
			 * check if UDF package is registered
			 */
			Node[] nodes = client.getNodes();
			String udfString = Info.request(nodes[0], "udf-list");
			
			if (!udfString.contains("claim-package")){
				log.debug("claim-package.lua is not regestered, registering now...");
				RegisterTask task = this.client.register(null, 
						"src/main/lua/claim-package.lua", 
						"claim-package.lua", 
						Language.LUA); 
				task.waitTillComplete();
				log.debug("claim-package.lua regesteration complete");
			}
			claimPackageRefistered = true;
		}

		// use date format 2012-07-04 in the URL 
		
		QueryPolicy policy = new QueryPolicy();
		Statement stmt = new Statement();
		stmt.setNamespace(namespace);
		stmt.setSetName(set);
		
		ResultSet resultSet = client.queryAggregate(null, stmt, 
				"claim-package", "check_claim_count" , Value.get(hospitalId));

		int count = 0;
		JSONObject jo = new JSONObject();
		JSONArray jArray = new JSONArray();
		try {
			
			
			while (resultSet.next()) {
				Object object = resultSet.getObject();
				jArray.add(object);
				System.out.println("Result: " + object);
				
				count++;
			}
			
			if (count == 0) {
				System.out.println("No results returned.");			
			}
		}
		finally {
			resultSet.close();
		}
		jo.put("Count", count);
		jo.put("Claims", jArray);

		
		return jo;
	}
	
	
	
	
	/**
	 * Query and filter by hospital Id
	 * This method assumes a secondary index on "hospital_index" 
	 * has already been created, and the package "claim-package.lua"
	 * has been registered
	 * @param namespace
	 * @param set
	 * @param hospitalId
	 * @param date
	 * @return
	 * @throws Exception
	 */
	@RequestMapping(value="/{namespace}/{set}/getProcedureByHospitalId/{hostpitalId}", method=RequestMethod.GET)
	public @ResponseBody JSONObject getCountByProcedure(@PathVariable("namespace") String namespace, 
			@PathVariable("set") String set, @PathVariable("hostpitalId") String hospitalId) throws Exception {

		/*
		 * create the data index once
		 */
		if (!procedureIndexCreated){
			/*
			 * check if the index is created
			 */
			Node[] nodes = client.getNodes();
			String infoResult = Info.request(nodes[0], "sindex");
			/*
			 * create if not found
			 */
			if (!infoResult.contains("procedure_index")){
				log.debug("The index: procedure_index has not been created, creating now...");
				IndexTask task = this.client.createIndex(null, namespace, set, "procedure_index", "Procedure", IndexType.STRING);
				task.waitTillComplete();
				log.debug("The index: procedure_index created successfully");
			}
			procedureIndexCreated = true;
		}
		/*
		 * register claim package once
		 */
		if (!groupByPackageRefistered){
			/*
			 * check if UDF package is registered
			 */
			Node[] nodes = client.getNodes();
			String udfString = Info.request(nodes[0], "udf-list");
			
			if (!udfString.contains("stream_udf")){
				log.debug("claim-package.lua is not regestered, registering now...");
				RegisterTask task = this.client.register(null, 
						"src/main/lua/stream_udf.lua", 
						"stream_udf.lua", 
						Language.LUA); 
				task.waitTillComplete();
				log.debug("stream_udf.lua regesteration complete");
			}
			groupByPackageRefistered = true;
		}

		// use date format 2012-07-04 in the URL 
		
		QueryPolicy policy = new QueryPolicy();
		Statement stmt = new Statement();
		stmt.setNamespace(namespace);
		stmt.setSetName(set);
		stmt.setFilters(Filter.equal("Hospital", hospitalId));
		
		ResultSet resultSet = client.queryAggregate(null, stmt, 
				"stream_udf", "group_count" , Value.get("Procedure"));

		int count = 0;
		JSONObject jo = new JSONObject();
		JSONArray jArray = new JSONArray();
		try {
			
			
			while (resultSet.next()) {
				Object object = resultSet.getObject();
				jArray.add(object);
				System.out.println("Result: " + object);
				
				count++;
			}
			
			if (count == 0) {
				System.out.println("No results returned.");			
			}
		}
		finally {
			resultSet.close();
		}
		jo.put("Claims", jArray);

		
		return jo;
	}

	/*
	 * CSV claims file upload
	 */
	@RequestMapping(value="/uploadClaims", method=RequestMethod.GET)
	public @ResponseBody String provideUploadInfo() {
		return "You can upload a file by posting to this same URL.";
	}

	@RequestMapping(value="/uploadClaims", method=RequestMethod.POST)
	public @ResponseBody String handleFileUpload(@RequestParam("name") String name, 
			@RequestParam("file") MultipartFile file){

		if (!file.isEmpty()) {
			try {
				WritePolicy wp = new WritePolicy();
				String line =  "";
				BufferedReader br = new BufferedReader(new InputStreamReader(file.getInputStream()));
				while ((line = br.readLine()) != null) {

					// use comma as separator
					String[] claim = line.split(",");

					/*
					 * write the record to Aerospike
					 * NOTE: Bin names must not exceed 14 characters
					 */
					client.put(wp,
							new Key("test", "claims",claim[0].trim() ),
							new Bin("Hospital", claim[1].trim()),	
							new Bin("IC_TPA", claim[2].trim()),
							new Bin("Claim_DATE", claim[3].trim()),
							new Bin("RequestedAmt", Integer.parseInt(claim[4].trim())),	
							new Bin("ApprovedAmt", Integer.parseInt(claim[5].trim())),	
							new Bin("Procedure", claim[6].trim()),	
							new Bin("Status", claim[7].trim())
							);

					log.debug("Claim [ID= " + claim[0] 
							+ " , hospital=" + claim[1] 
									+ " , IC_TPA=" + claim[2] 
											+ " , Claim_DATE=" + claim[3] 
													+ " , RequestedAmt=" + claim[4] 
															+ " , Procedure=" + claim[5] 
																	+ " , Status=" + claim[6] 
																					+ "]");

				}
				br.close();
				log.info("Successfully uploaded " + name);
				return "You successfully uploaded " + name;
			} catch (Exception e) {
				log.error("Failed to upload " + name, e);
				return "You failed to upload " + name + " => " + e.getMessage();
			}
		} else {
			log.info("Failed to upload " + name + " because the file was empty.");
			return "You failed to upload " + name + " because the file was empty.";
		}
	}

}
