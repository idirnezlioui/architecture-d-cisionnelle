def write_data_minio():
    """
    This method put all Parquet files into Minio
    Ne pas faire cette méthode pour le moment
    """
    client = Minio(
        "http://127.0.0.1:9000/",
        secure=False,
        access_key="minioadmin",
        secret_key="minioadmin"
    )
    bucket: str = "bucket_datamart"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")
    LOCAL_PARQUET_DIRECTORY = "E:/Ingénierie 1 EISI/Architecture Décisionnelle/TP Datamart/taxi_data"
    # Parcourez les fichiers Parquet dans le répertoire local
    for file_name in os.listdir(LOCAL_PARQUET_DIRECTORY):
        if file_name.endswith('.parquet'):
            local_file_path = os.path.join(LOCAL_PARQUET_DIRECTORY, file_name)
            remote_object_name = f"{file_name}"
            # Transférez le fichier Parquet vers MinIO
            with open(local_file_path, 'rb') as file_data:
                client.put_object(bucket, remote_object_name, file_data, length=os.stat(local_file_path).st_size)
            print(f"Le fichier {file_name} a été transféré avec succès vers {bucket} sur MinIO.")
if name == 'main':
    sys.exit(main())
main()