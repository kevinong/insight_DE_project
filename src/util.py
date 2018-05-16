def get_s3_path(bucket_name, folder_name=None, file_name=None):
    path = 's3a://{}/'.format(bucket_name)

    if folder_name is not None:
        path += '{}/'.format(folder_name)
    if file_name is not None:
        path += '{}/'.format(file_name)

    return path