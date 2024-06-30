# Data Ingestion

## Lambda function
- The lambda function requires a lambda layer. Steps to create and add lambda layer are included below.
- This lambda function ingests data from an S3 bucket hosted by WCD. WCD has required AWS authentication to access the bucket to avoid unknown public requests to the bucket. The bucket has also been configured as "requester pays" to avoid WCD incurring charges from public access to the bucket.
- This function was built with python3.10
- The function takes a few minutes to fully ingest the data. Increasing the memory can make the function faster, or set the lambda function run time to a maximum of 15 minutes to allow it to run to completion.

## Creating a lambda layer

1. create layer directory
mkdir -p lambda_layers/python/lib/python3.10/site-packages

2. create virtual environment 
python3 -m venv venv
source venv/bin/activate

3. install the dependencies in the desired folder using the requirements.txt file
pip3 install -r requirements.txt -t lambda_layers/python/lib/python3.10/site-packages/.

to install snowflake-connector-python package we are not using requirements.txt file. The pip3 install command is also using multiple flags. This is because lambda backend containers are using older packages which makes it difficult to install it using other methods.
#Refer to this link for more info: (https://stackoverflow.com/questions/75472308/aws-lambda-returns-lib64-libc-so-6-version-glibc-2-28-not-found) 

pip3 install --platform manylinux2010_x86_64 --implementation cp --python 3.10 --only-binary=:all: --upgrade --target lambda_layers/python/lib/python3.10/site-packages snowflake-connector-python==2.7.9

4. zip the lambda_layers folder
cd lambda_layers
zip -r snowflake_lambda_layer.zip *

5. publish layer directly to lambda
aws lambda publish-layer-version \
    --layer-name snowflake-lambda-layer \
    --compatible-runtimes python3.10 \
    --zip-file fileb://snowflake_lambda_layer.zip

OR copy to S3 and create layer through aws console:

aws s3 cp snowflake_lambda_layer.zip s3://lambda-layer-bucket
