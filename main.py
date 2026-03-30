import tokens
import boto3
import time 

def main():
    session = tokens.get_aws_session()
    lambda_client = session.client("lambda",region_name="ap-southeast-3")
    time.sleep(950)
    page = lambda_client.get_paginator('list_functions')
    count=0
    for i in page.paginate():
        if count ==5: break
        print(i["Functions"])
        count+=1



if __name__ == "__main__":
    main()
