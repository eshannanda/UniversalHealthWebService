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

import org.json.simple.JSONObject;

import com.aerospike.client.Record;

/**
 * JSONRecord is used to convert an Aerospike Record
 * returned from the cluster to JSON format
 * @author peter
 *
 */
@SuppressWarnings("serial")
public class JSONRecord extends JSONObject {
	@SuppressWarnings("unchecked")
	public JSONRecord(Record record){
		put("generation", record.generation);
		put("expiration", record.expiration);
		put("bins", new JSONObject(record.bins));
	}

}