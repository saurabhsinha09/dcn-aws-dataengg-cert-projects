# Docker Authentication with ECR
aws ecr get-login-password \
        --region ap-south-1 | docker login \
        --username AWS \
        --password-stdin <aws-account-id>.dkr.ecr.ap-south-1.amazonaws.com

# Commands for data-validation task  
docker build -t ecom_data_validation .
docker run -d -v ~/.aws:/root/.aws ecom_data_validation
docker tag ecom_data_validation:latest <aws-account-id>.dkr.ecr.ap-south-1.amazonaws.com/dcn-ecommerce-pipelines:ecom_data_validation
docker push <aws-account-id>.dkr.ecr.ap-south-1.amazonaws.com/dcn-ecommerce-pipelines:ecom_data_validation

# Newly tagged image 
docker build -t ecom_data_validation .
docker tag ecom_data_validation:latest <aws-account-id>.dkr.ecr.ap-south-1.amazonaws.com/dcn-ecommerce-pipelines:ecom_data_validation_v1
docker push <aws-account-id>.dkr.ecr.ap-south-1.amazonaws.com/dcn-ecommerce-pipelines:ecom_data_validation_v1

docker build -t ecom_data_validation .
docker tag ecom_data_validation:latest <aws-account-id>.dkr.ecr.ap-south-1.amazonaws.com/dcn-ecommerce-pipelines:ecom_data_validation_v2
docker push <aws-account-id>.dkr.ecr.ap-south-1.amazonaws.com/dcn-ecommerce-pipelines:ecom_data_validation_v2

# Commands for ETL Job
docker build -t etl_aggregations . 
docker run -d -v ~/.aws:/root/.aws etl_aggregations
docker tag etl_aggregations:latest <aws-account-id>.dkr.ecr.ap-south-1.amazonaws.com/dcn-ecommerce-pipelines:etl_aggregations
docker push <aws-account-id>.dkr.ecr.ap-south-1.amazonaws.com/dcn-ecommerce-pipelines:etl_aggregations