import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "demo", table_name = "demo_incidents", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "demo", table_name = "demo_incidents", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("vehicle4", "string", "vehicle4", "string"), ("contrfact1", "string", "contrfact1", "string"), ("month", "string", "month", "string"), ("weather", "string", "weather", "string"), ("fatalities", "string", "fatalities", "string"), ("numpassengers", "int", "numpassengers", "int"), ("year", "string", "year", "string"), ("injuries", "string", "injuries", "string"), ("ta_date", "string", "ta_date", "string"), ("zone", "string", "zone", "string"), ("vehicle1", "string", "vehicle1", "string"), ("vehicle2", "string", "vehicle2", "string"), ("vehicle3", "string", "vehicle3", "string"), ("lon", "double", "lon", "double"), ("vehicle5", "string", "vehicle5", "string"), ("contributing_factor", "array", "contributing_factor", "string"), ("geo_location.lat", "double", "`geo_location.lat`", "double"), ("geo_location.lon", "double", "`geo_location.lon`", "double"), ("rdconfigur", "array", "rdconfigur", "string"), ("trafcontrl", "string", "trafcontrl", "string"), ("ta_time", "string", "ta_time", "string"), ("contrcir4_desc", "string", "contrcir4_desc", "string"), ("vehicleconcat2", "string", "vehicleconcat2", "string"), ("vehicleconcat3", "string", "vehicleconcat3", "string"), ("rdfeature", "string", "rdfeature", "string"), ("rdcondition", "string", "rdcondition", "string"), ("contrcir2_desc", "string", "contrcir2_desc", "string"), ("lightcond", "string", "lightcond", "string"), ("contrcir3_desc", "string", "contrcir3_desc", "string"), ("records", "int", "records", "int"), ("contrfact2", "string", "contrfact2", "string"), ("fatality", "int", "fatality", "int"), ("tract", "string", "tract", "string"), ("lat", "double", "lat", "double"), ("crash_date", "string", "crash_date", "string"), ("vehicle_type", "array", "vehicle_type", "string"), ("possblinj", "int", "possblinj", "int"), ("rdsurface", "string", "rdsurface", "string"), ("tamainid", "int", "tamainid", "int"), ("rdclass", "array", "rdclass", "string"), ("lat2", "double", "lat2", "double"), ("contrcir1_desc", "string", "contrcir1_desc", "string"), ("rdcharacter", "array", "rdcharacter", "string"), ("location_description", "string", "location_description", "string"), ("workarea", "string", "workarea", "string"), ("vehicleconcat1", "string", "vehicleconcat1", "string"), ("lon2", "double", "lon2", "double"), ("numpedestrians", "string", "numpedestrians", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("vehicle4", "string", "vehicle4", "string"), ("contrfact1", "string", "contrfact1", "string"), ("month", "string", "month", "string"), ("weather", "string", "weather", "string"), ("fatalities", "string", "fatalities", "string"), ("numpassengers", "int", "numpassengers", "int"), ("year", "string", "year", "string"), ("injuries", "string", "injuries", "string"), ("ta_date", "string", "ta_date", "string"), ("zone", "string", "zone", "string"), ("vehicle1", "string", "vehicle1", "string"), ("vehicle2", "string", "vehicle2", "string"), ("vehicle3", "string", "vehicle3", "string"), ("lon", "double", "lon", "double"), ("vehicle5", "string", "vehicle5", "string"), ("contributing_factor", "array", "contributing_factor", "string"), ("geo_location.lat", "double", "`geo_location.lat`", "double"), ("geo_location.lon", "double", "`geo_location.lon`", "double"), ("rdconfigur", "array", "rdconfigur", "string"), ("trafcontrl", "string", "trafcontrl", "string"), ("ta_time", "string", "ta_time", "string"), ("contrcir4_desc", "string", "contrcir4_desc", "string"), ("vehicleconcat2", "string", "vehicleconcat2", "string"), ("vehicleconcat3", "string", "vehicleconcat3", "string"), ("rdfeature", "string", "rdfeature", "string"), ("rdcondition", "string", "rdcondition", "string"), ("contrcir2_desc", "string", "contrcir2_desc", "string"), ("lightcond", "string", "lightcond", "string"), ("contrcir3_desc", "string", "contrcir3_desc", "string"), ("records", "int", "records", "int"), ("contrfact2", "string", "contrfact2", "string"), ("fatality", "int", "fatality", "int"), ("tract", "string", "tract", "string"), ("lat", "double", "lat", "double"), ("crash_date", "string", "crash_date", "string"), ("vehicle_type", "array", "vehicle_type", "string"), ("possblinj", "int", "possblinj", "int"), ("rdsurface", "string", "rdsurface", "string"), ("tamainid", "int", "tamainid", "int"), ("rdclass", "array", "rdclass", "string"), ("lat2", "double", "lat2", "double"), ("contrcir1_desc", "string", "contrcir1_desc", "string"), ("rdcharacter", "array", "rdcharacter", "string"), ("location_description", "string", "location_description", "string"), ("workarea", "string", "workarea", "string"), ("vehicleconcat1", "string", "vehicleconcat1", "string"), ("lon2", "double", "lon2", "double"), ("numpedestrians", "string", "numpedestrians", "string")], transformation_ctx = "applymapping1")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://glue-demo-bucket/data/output"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": "s3://glue-demo-bucket/data/output"}, format = "csv", transformation_ctx = "datasink2")
job.commit()
