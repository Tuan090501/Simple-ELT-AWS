import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node artist
artist_node1719105515261 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-dw/staging/artists.csv"], "recurse": True}, transformation_ctx="artist_node1719105515261")

# Script generated for node track
track_node1719105515735 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-dw/staging/track.csv"], "recurse": True}, transformation_ctx="track_node1719105515735")

# Script generated for node album
album_node1719105514747 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://project-spotify-dw/staging/albums.csv"], "recurse": True}, transformation_ctx="album_node1719105514747")

# Script generated for node Join Album &  Artist
JoinAlbumArtist_node1719105620915 = Join.apply(frame1=album_node1719105514747, frame2=artist_node1719105515261, keys1=["artist_id"], keys2=["id"], transformation_ctx="JoinAlbumArtist_node1719105620915")

# Script generated for node Join with Track
JoinwithTrack_node1719105676012 = Join.apply(frame1=track_node1719105515735, frame2=JoinAlbumArtist_node1719105620915, keys1=["track_id"], keys2=["track_id"], transformation_ctx="JoinwithTrack_node1719105676012")

# Script generated for node Drop Fields
DropFields_node1719106074086 = DropFields.apply(frame=JoinwithTrack_node1719105676012, paths=["`.track_id`", "id"], transformation_ctx="DropFields_node1719106074086")

# Script generated for node Destination
Destination_node1719106095180 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1719106074086, connection_type="s3", format="glueparquet", connection_options={"path": "s3://project-spotify-dw/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Destination_node1719106095180")

job.commit()