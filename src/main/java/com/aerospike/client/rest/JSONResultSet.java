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

import org.json.simple.JSONArray;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Record;
import com.aerospike.client.query.ResultSet;

@SuppressWarnings("serial")
public class JSONResultSet extends JSONArray{
	@SuppressWarnings("unchecked")
	public JSONResultSet(ResultSet resultSet) throws AerospikeException{
		super();
		try {
			while (resultSet.next()) {
				Record record =  (Record) resultSet.getObject();
				add(new JSONRecord(record));
			}
		} finally {
			resultSet.close();
		}
	}
}
