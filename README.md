# UniversalHealthWebService
Spring Boot project for pushing and retrieving data from AeroSpike

Spring Boot application for rest services

You can change the configuration of aerospike in AerospikeRESTfulService.java

This provide way to push the data into aerospike using csv file
CSV format - claimID,hospitalID,TPA_ID,RequestedAmount,ApprovedAmount,Procedure,Status
Run JUnit ClaimsUploader.java for pushing the data

Allows for retrieving data of a particular claim
http://localhost:8080/test/claims/getAll/28   --> 28 is the claimID

Allows for retrieving data of a hospital along with count 
http://localhost:8080/test/claims/getClaimsByHospitalId/24  --> 24 is hospitalId

Allows for retrieving data of a hospital group by procedure
http://localhost:8080/test/claims/getProcedureByHospitalId/1 
