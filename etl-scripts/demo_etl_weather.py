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
## @args: [database = "demo", table_name = "demo_weather", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "demo", table_name = "demo_weather", transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("rowid", "string", "rowid", "string"), ("datetime", "string", "datetime", "string"), ("tempout", "double", "tempout", "double"), ("hitemp", "double", "hitemp", "double"), ("lowtemp", "double", "lowtemp", "double"), ("outhum", "long", "outhum", "long"), ("dewpt", "double", "dewpt", "double"), ("windspeed", "long", "windspeed", "long"), ("winddir", "string", "winddir", "string"), ("windrun", "double", "windrun", "double"), ("hispeed", "long", "hispeed", "long"), ("hidir", "string", "hidir", "string"), ("windchill", "double", "windchill", "double"), ("heatindex", "double", "heatindex", "double"), ("thwindex", "double", "thwindex", "double"), ("bar", "double", "bar", "double"), ("rain", "long", "rain", "long"), ("rainrate", "long", "rainrate", "long"), ("heatdd", "double", "heatdd", "double"), ("cooldd", "long", "cooldd", "long"), ("intemp", "double", "intemp", "double"), ("inhum", "long", "inhum", "long"), ("indew", "double", "indew", "double"), ("inheat", "double", "inheat", "double"), ("inemc", "double", "inemc", "double"), ("inairdensity", "double", "inairdensity", "double"), ("windsamp", "long", "windsamp", "long"), ("windtx", "long", "windtx", "long"), ("issrecpt", "long", "issrecpt", "long"), ("arcint", "long", "arcint", "short")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("rowid", "string", "rowid", "string"), ("datetime", "string", "datetime", "string"), ("tempout", "double", "tempout", "double"), ("hitemp", "double", "hitemp", "double"), ("lowtemp", "double", "lowtemp", "double"), ("outhum", "long", "outhum", "long"), ("dewpt", "double", "dewpt", "double"), ("windspeed", "long", "windspeed", "long"), ("winddir", "string", "winddir", "string"), ("windrun", "double", "windrun", "double"), ("hispeed", "long", "hispeed", "long"), ("hidir", "string", "hidir", "string"), ("windchill", "double", "windchill", "double"), ("heatindex", "double", "heatindex", "double"), ("thwindex", "double", "thwindex", "double"), ("bar", "double", "bar", "double"), ("rain", "long", "rain", "long"), ("rainrate", "long", "rainrate", "long"), ("heatdd", "double", "heatdd", "double"), ("cooldd", "long", "cooldd", "long"), ("intemp", "double", "intemp", "double"), ("inhum", "long", "inhum", "long"), ("indew", "double", "indew", "double"), ("inheat", "double", "inheat", "double"), ("inemc", "double", "inemc", "double"), ("inairdensity", "double", "inairdensity", "double"), ("windsamp", "long", "windsamp", "long"), ("windtx", "long", "windtx", "long"), ("issrecpt", "long", "issrecpt", "long"), ("arcint", "long", "arcint", "short")], transformation_ctx = "applymapping1")
## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://glue-demo-bucket/data/output"}, format = "csv", transformation_ctx = "datasink2"]
## @return: datasink2
## @inputs: [frame = applymapping1]
datasink2 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": "s3://glue-demo-bucket/data/output"}, format = "csv", transformation_ctx = "datasink2")
job.commit()
