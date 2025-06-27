from diagrams import Diagram, Cluster
from diagrams.aws.analytics import Glue
from diagrams.aws.storage import S3
from diagrams.aws.database import Redshift
from diagrams.aws.management import Cloudwatch
from diagrams.custom import Custom

# Local icon for GitHub Actions (must be in your working directory)
gha_icon = "GitHub_Actions.png"

with Diagram(
    "Wistia Video Analytics Data Pipeline",
    show=False,
    direction="LR",
    graph_attr={"splines": "spline", "ranksep": "2.0"},  # Increased spacing
):
    # CI/CD
    github_actions = Custom("CI/CD (GitHub Actions)", "./github.png")

    # Glue Processing
    with Cluster("AWS Glue"):
        glue_crawler = Glue("Glue Crawler")
        glue_script_job = Glue("Glue Job (Wistia API + Transform)")
        

    # Unified S3 Storage Bucket
    with Cluster("Amazon S3: wistiaproject"):
        s3_bucket = S3("Root Bucket")
        s3_prefixes = [
            S3("dim_media/"),
            S3("fact_engagements/"),
            S3("fact_engagements_byday/"),
            S3("fact_events/"),
            S3("dim_visitors/"),
            S3("fact_engagement_graph/")
        ]

    # Downstream Consumption
    redshift_dw = Redshift("Redshift (Data Warehouse & Analytics)")
    

    # Flow
    github_actions >> glue_script_job
    glue_script_job >> s3_bucket
    s3_bucket >> s3_prefixes >> glue_crawler >> redshift_dw
    
