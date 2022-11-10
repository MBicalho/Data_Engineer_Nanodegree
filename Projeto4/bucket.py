import boto3
import configparser

def create_s3_bucket(s3, bucket_root, keys):

     """
    Create the bucket and the folders in S3
    
    Parameters:
    s3: object.
    
    bucket_root: Bucket name to be created.
    
    keys: the folder list to create.
    """

    try:
        s3.create_bucket(ACL='private',Bucket=bucket_root,
                             CreateBucketConfiguration={'LocationConstraint':'us-west-2'})
    except Exception as e:
        if 'BucketAlreadyOwnedByYou' in str(e):
            print(f'{bucket_root} already exists')
        else:
            raise e
    buck = s3.Bucket(bucket_root)


    for key in keys:
        key_name = f'{key}/'
        try: 
            buck.put_object(Key=key_name)
        except Exception as e:
            raise e
    print('Folder list created')
            
if __name__=='__main__':
    
    config = configparser.ConfigParser()
    config.read_file(open('dl.cfg'))
    
    KEY=config.get('AWS','AWS_ACCESS_KEY_ID')
    SECRET= config.get('AWS','AWS_SECRET_ACCESS_KEY')

    
    s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                     )
    
    create_s3_bucket(s3, bucket_root='udacity-matheus-data-lake', 
                     keys = ['songs','songplays','time','artists','users'])